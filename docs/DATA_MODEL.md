# Diagramme de la base de donnée

https://dbdiagram.io/d/6a1d87582eeb2f46cd329459

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
