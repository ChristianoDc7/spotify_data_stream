# Diagramme de la base de donnée

https://dbdiagram.io/d/6a1d87582eeb2f46cd329459

---

## Présentation des tables

### Catalogue musical

| Table | Rôle |
|-------|------|
| `genres` | Liste de référence des genres musicaux (Pop, Rock, Jazz...). Sert à normaliser les genres dans tout le catalogue. |
| `artists` | Les artistes référencés sur la plateforme, avec leur label, pays et nombre d'auditeurs mensuels. C'est la table racine du catalogue. |
| `albums` | Les albums de chaque artiste. Chaque album appartient à un artiste et contient plusieurs tracks. |
| `tracks` | Les morceaux disponibles sur la plateforme avec leur durée, genre, BPM et chemin vers le fichier audio sur MinIO. C'est la table la plus consultée par les pipelines. |

### Réseau P2P et utilisateurs

| Table | Rôle |
|-------|------|
| `peers` | Les appareils connectés au réseau P2P (mobile, desktop, enceinte connectée...). Chaque peer peut servir des morceaux en cache aux autres. |

### Événements d'écoute

| Table | Rôle |
|-------|------|
| `listening_events` | Chaque ligne représente une écoute : quel utilisateur a écouté quel morceau, depuis quel appareil, depuis quel pays, pendant combien de temps. C'est la table centrale de la plateforme — tout le reste en dérive. |

### Agrégats batch

| Table | Rôle |
|-------|------|
| `daily_streams` | Les statistiques quotidiennes par morceau (nombre de streams, auditeurs uniques, durée totale). Calculées chaque nuit par `aggregation_pipeline`. C'est la source de vérité pour les royalties et les charts officiels. |
| `artist_stats` | Les statistiques quotidiennes par artiste (streams, auditeurs uniques, morceau phare du jour). Calculées en même temps que `daily_streams`. |
| `recommendations` | Le top-10 de recommandations par utilisateur généré par `recommendation_pipeline`. Persisté ici pour l'historique et l'audit, Redis étant le cache d'accès rapide. |

### Résilience

| Table | Rôle |
|-------|------|
| `dead_letter_events` | La "poubelle intelligente" : les événements invalides rejetés par les pipelines y sont stockés avec le message d'erreur, puis retraités périodiquement par `dlq_reprocessing_pipeline`. Rien n'est perdu définitivement. |

### Temps réel (Phase 2 — alimentées par Spark)

| Table | Rôle |
|-------|------|
| `realtime_top_tracks` | Le classement des morceaux mis à jour toutes les 5 minutes par Spark Streaming. Contrairement à `daily_streams` qui est figée à J-1, cette table reflète les tendances de la dernière heure. |
| `fraud_detections` | Les alertes générées par le job de détection de fraude Spark : bots qui streament en rafale, comportements suspects. Chaque alerte contient le type de fraude, le score de suspicion et les preuves. |

### Fédération inter-groupes (Phase 3)

| Table | Rôle |
|-------|------|
| `federated_catalog` | Les morceaux partagés par les autres groupes via le réseau P2P inter-groupes. Permet à notre plateforme de proposer des tracks qui ne viennent pas de nos 3 labels. |

---

## Questions de modélisation

### Pourquoi `listening_events` a deux index sur le timestamp ?

```sql
CREATE INDEX idx_listening_events_timestamp        ON listening_events(timestamp);
CREATE INDEX idx_listening_events_ts_partition     ON listening_events(date_trunc('hour', timestamp));
```

Les deux index répondent à des patterns de requêtes différents :

- **`idx_listening_events_timestamp`** — index classique sur la colonne brute. Utile pour les requêtes de plage (`WHERE timestamp BETWEEN x AND y`), les tris, et les JOINs temporels. PostgreSQL peut utiliser un index scan sur n'importe quel intervalle.

- **`idx_listening_events_ts_partition`** — index fonctionnel sur `date_trunc('hour', timestamp)`. Les DAGs Airflow d'agrégation traitent les événements fenêtre par fenêtre horaire avec une clause du type `WHERE date_trunc('hour', timestamp) = '2025-01-15 14:00:00'`. Sans cet index, PostgreSQL doit scanner toute la table et calculer `date_trunc` pour chaque ligne. Avec l'index, il retrouve directement les lignes de l'heure concernée.

En résumé : le premier sert les requêtes ad-hoc sur des plages arbitraires, le second optimise spécifiquement le traitement batch horaire d'Airflow.

---

### Quelle est la différence entre `daily_streams` et `realtime_top_tracks` ?

|                   | `daily_streams`                                      | `realtime_top_tracks`                                                                           |
| ----------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| **Couche Lambda** | Batch layer                                          | Speed layer                                                                                     |
| **Producteur**    | Airflow DAG (agrégation SQL)                         | Spark Structured Streaming                                                                      |
| **Granularité**   | Journalière — `(track_id, date)`                     | Fenêtres glissantes de 5 min — `(window_start, track_id)`                                       |
| **Latence**       | Haute (données de la veille)                         | Basse (~5 min de délai)                                                                         |
| **Complétude**    | Complète — tous les événements du jour sont intégrés | Approximative — les événements tardifs (`late_listening_events`) ne sont pas encore réconciliés |
| **Usage**         | Rapports, royalties, stats historiques               | Dashboard temps réel, top charts live                                                           |

`daily_streams` est la source de vérité pour tout ce qui touche à la facturation ou aux stats officielles. `realtime_top_tracks` sert l'affichage live mais peut légèrement sous-compter si des événements arrivent en retard.

---

### Pourquoi `dead_letter_events.payload` est `JSONB` et pas `TEXT` ?

`TEXT` stockerait le JSON comme une chaîne opaque — impossible d'interroger le contenu sans le parser applicativement à chaque fois.

`JSONB` (JSON Binaire) offre trois avantages concrets pour une DLQ :

1. **Requêtes directes sur le contenu** — lors du retraitement, on peut filtrer par type d'événement sans charger tous les enregistrements dans l'application :
    ```sql
    SELECT * FROM dead_letter_events
    WHERE payload->>'event_type' = 'listening'
      AND status = 'pending';
    ```

```

2. **Indexation GIN** — si le volume de DLQ devient important, on peut ajouter `CREATE INDEX ON dead_letter_events USING GIN (payload)` pour accélérer les recherches sur n'importe quelle clé JSON.

3. **Validation à l'insertion** — PostgreSQL rejette tout JSON malformé au moment du `INSERT`, ce qui garantit que ce qui entre dans la DLQ est au moins syntaxiquement valide et re-parseable par le DAG de retraitement.
```
