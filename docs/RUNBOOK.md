# RUNBOOK SPOTIFY — Procédures incidents

> Ce document doit être complété par votre groupe au fur et à mesure de la semaine.
> Un bon runbook = ce dont vous auriez eu besoin pendant la panne.

---

## Incidents Phase 1 — Airflow / Batch

### INC-01 — DAG bloqué en `queued` sans démarrer

**Symptômes :** Un DAGRun reste en état `queued` dans l'UI sans jamais passer en `running`.

**Causes fréquentes :**
- Le DAG est **paused** (toggle gris dans l'UI)
- Le Celery worker n'est pas connecté à Redis (broker down ou mauvais port)

**Diagnostic :**
```bash
# Vérifier l'état des containers
docker compose ps

# Vérifier que le worker est connecté
docker logs spotify_data_stream-airflow-worker-1 2>&1 | tail -5
# Doit afficher : "Connected to redis://redis:6379/0" et "celery@... ready."

# Vérifier si le DAG est paused
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags list | grep <dag_id>
```

**Résolution :**
```bash
# Unpause le DAG
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags unpause <dag_id>

# Si le worker est down
docker compose restart airflow-worker

# Retrigger le run
docker exec spotify_data_stream-airflow-scheduler-1 airflow dags trigger <dag_id>
```

---

### INC-02 — Conflit de port entre service local et Docker (Redis / PostgreSQL)

**Symptômes :**
- Le simulateur P2P publie des events mais le DAG consomme 0 événements
- `psql` retourne `role "spotify" does not exist` malgré le container postgres healthy

**Cause :** Un service Mac local (PostgreSQL ou Redis installé via Homebrew) est déjà en écoute sur le même port que Docker (`5432` ou `6379`). Les deux services coexistent mais `localhost` résout vers le service local.

**Diagnostic :**
```bash
# Voir tous les processus sur le port
lsof -i :6379   # ou :5432
# Si deux lignes : un process "postgres"/"redis-ser" ET "com.docke" → conflit
```

**Résolution :**
```bash
# Changer le port exposé dans docker-compose.yml
# Redis : "6380:6379" au lieu de "6379:6379"
# PostgreSQL : "5433:5432" au lieu de "5432:5432"

# Mettre à jour REDIS_URL dans le simulateur
# REDIS_URL = "redis://localhost:6380/1"

docker compose down && docker compose up -d
```

---

### INC-03 — Inserts PostgreSQL silencieusement ignorés (FK violation)

**Symptômes :** Le DAG tourne vert, `upsert_to_postgres` retourne `inserted=0, skipped=N` sans erreur visible. `SELECT COUNT(*) FROM listening_events` reste à 0.

**Cause :** Une contrainte de clé étrangère (`source_peer_id → peers`) rejette les inserts car le simulateur génère des UUIDs aléatoires non présents dans la table `peers`.

**Diagnostic :**
```bash
# Vérifier les contraintes FK de la table
docker exec spotify_data_stream-postgres-1 psql -U airflow -d spotify -c "\d listening_events"

# Tester un insert manuel pour voir l'erreur brute
docker exec spotify_data_stream-postgres-1 psql -U airflow -d spotify -c \
  "INSERT INTO listening_events (id, user_id, track_id, timestamp, duration_ms) VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), NOW(), 30000);"
```

**Résolution (Phase 1 — données simulées) :**
```bash
# Supprimer la contrainte FK temporairement
docker exec spotify_data_stream-postgres-1 psql -U airflow -d spotify -c \
  "ALTER TABLE listening_events DROP CONSTRAINT listening_events_source_peer_id_fkey;"
```

**Prévention (Phase 2) :** Charger d'abord le catalogue (issue #4) pour que les `track_id` et `peer_id` existent réellement dans les tables référencées avant d'activer les FK.

---

## Incidents Phase 2 — Kafka / Spark

### INC-04 — Consumer lag Kafka qui explose

**Symptômes :** Kafka UI → consumer group `spark-streaming-trends` → lag > 10 000

**Diagnostic :**
```bash
# Vérifier le throughput Spark
docker logs spark-master -f | grep "Batch Duration"

# Vérifier les ressources
docker stats spark-worker-1
```

**Résolution :**
→ À compléter par votre groupe

---

### INC-05 — Job Spark crash avec OutOfMemory

**Symptômes :** `java.lang.OutOfMemoryError: GC overhead limit exceeded`

**Diagnostic :**
```bash
docker logs spark-master -f | grep -i "error\|exception\|oom"
```

**Résolution :**
```bash
# Augmenter la mémoire du worker dans docker-compose
# SPARK_WORKER_MEMORY: 4G

# Réduire le state store : ajouter un TTL sur flatMapGroupsWithState
# GroupState.setTimeoutDuration("1 hour")
```

---

### INC-06 — Spark ne reprend pas depuis le checkpoint

**Symptômes :** Après redémarrage, le job repart de zéro au lieu du checkpoint.

**Diagnostic :**
```bash
# Vérifier que le checkpoint est sur MinIO
docker exec minio mc ls local/spotify-checkpoints/streaming_trends/

# Vérifier les logs Spark au démarrage
docker logs spark-master | grep "checkpoint"
```

**Résolution :**
→ À compléter par votre groupe

---

## Chaos Engineering — Résultats

> Compléter pendant l'issue #25 (vendredi)

### Scénario 1 : Arrêt d'un broker Kafka

**Commande :** `docker compose stop kafka-2`

**Comportement observé :** ...

**Recovery automatique :** oui / non — détails : ...

**Temps de recovery :** ...

---

### Scénario 2 : Kill du driver Spark

**Commande :** `docker compose kill spark-master`

**Comportement observé :** ...

**Recovery depuis checkpoint :** oui / non — détails : ...

**Doublons introduits :** 0 / N — vérification : ...

---

### Scénario 3 : Coupure PostgreSQL

**Commande :** `docker compose stop postgres` (2 minutes) → `docker compose start postgres`

**Comportement observé (Airflow) :** ...

**Comportement observé (Spark) :** ...

**Données perdues :** oui / non — détails : ...
