"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (pub/sub),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)

Architecture :
    Redis (pub/sub listening_events + p2p_network_events)
        → consume_from_redis()
        → validate_events()          ← invalides → DLQ
        → enrich_events()            ← jointure catalogue PostgreSQL
        → store_to_parquet()         ← MinIO partitionné par heure
        → upsert_to_postgres()       ← table listening_events

TODO :
    [ ] Implémenter consume_from_redis() — accumuler les events sur 5 min
    [ ] Implémenter validate_events() — champs obligatoires, envoyer invalides en DLQ
    [ ] Implémenter enrich_events() — joindre avec le catalogue (track_id → artiste, genre)
    [ ] Implémenter store_to_parquet() — Parquet sur MinIO partitionné par heure
    [ ] Implémenter upsert_to_postgres() — insérer dans listening_events
    [ ] Utiliser TaskFlow API (@task) pour toutes les tâches
    [ ] Ajouter des branches conditionnelles : séparer listening_events et p2p_network_events
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis channel `listening_events`
- Redis channel `p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID). L'upsert utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.

### TODO
Compléter les 5 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_CHANNELS   = ["listening_events", "p2p_network_events"]
BATCH_WINDOW_SEC = 300  # 5 minutes


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """
        Consomme les événements Redis publiés pendant la fenêtre de 5 minutes.

        TODO :
            1. Se connecter à Redis (REDIS_URL depuis les env vars)
            2. Utiliser un pattern subscriber ou lire depuis une liste Redis
               (le simulateur publie sur les channels REDIS_CHANNELS)
            3. Accumuler tous les messages de la fenêtre temporelle
            4. Retourner {"listening": [...], "p2p_network": [...]}

        Hint : avec redis pub/sub, les messages ne sont pas persistés.
        Une alternative : le simulateur peut aussi écrire dans une Redis LIST
        (lpush) que le DAG consomme avec rpop/lrange.
        Discutez avec l'équipe Infra & P2P de la stratégie choisie.
        """
        raise NotImplementedError("TODO : implémenter consume_from_redis()")

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les événements et isole les invalides en DLQ.

        Champs obligatoires pour un listening_event :
            event_id, user_id, track_id, timestamp, duration_ms

        TODO :
            1. Parcourir raw_events["listening"] et raw_events["p2p_network"]
            2. Valider les champs obligatoires
            3. Valider les types (timestamp parseable, duration_ms > 0)
            4. Invalides → INSERT dans dead_letter_events avec error_type="validation"
            5. Retourner {"valid_listening": [...], "valid_p2p": [...], "errors": N}
        """
        raise NotImplementedError("TODO : implémenter validate_events()")

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les événements d'écoute avec les données du catalogue.

        TODO :
            1. Charger les tracks depuis PostgreSQL (batch query par track_id)
               SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%(ids)s)
            2. Pour chaque listening_event, ajouter : genre, artist_id, track_title
            3. Les track_id inconnus → DLQ avec error_type="unknown_track"
            4. Retourner la liste des events enrichis

        Hint : faire une seule requête PostgreSQL avec IN clause plutôt qu'une par event.
        """
        raise NotImplementedError("TODO : implémenter enrich_events()")

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Sauvegarde les événements enrichis en Parquet sur MinIO.

        Partitionnement : date + heure (pour la parallélisation Phase 1, seq 3.1)

        TODO :
            1. Convertir la liste d'events en DataFrame pandas
            2. Partitionner par date et heure du timestamp
            3. Écrire en Parquet sur MinIO via boto3 ou pyarrow
               Chemin : s3://spotify-parquet/listening_events/date={date}/hour={hour}/part-{run_id}.parquet
            4. Retourner le chemin du fichier écrit

        Hint : pyarrow.parquet.write_table() + boto3 pour l'upload
        """
        raise NotImplementedError("TODO : implémenter store_to_parquet()")

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les événements dans PostgreSQL de façon idempotente.

        TODO :
            1. Utiliser PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            2. INSERT INTO listening_events (...) VALUES ...
               ON CONFLICT (id) DO NOTHING
            3. Retourner {"inserted": N, "skipped": M}

        Hint : utiliser executemany() avec des tuples pour les performances.
        """
        raise NotImplementedError("TODO : implémenter upsert_to_postgres()")

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
