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
"""

import io
import json
import logging
import os
import uuid
from datetime import datetime, timedelta

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import redis as redis_lib
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis LIST `listening_events_queue`
- Redis LIST `p2p_network_events_queue`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID). L'upsert utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.
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
REDIS_URL        = os.environ.get("REDIS_URL", "redis://redis:6379/1")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

LISTENING_REQUIRED = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}
P2P_REQUIRED       = {"event_id", "event_type", "peer_id", "timestamp"}


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
        Draine les queues Redis accumulées depuis le dernier run.
        Le simulateur pousse dans listening_events_queue et p2p_network_events_queue
        (Redis LIST) en plus du pub/sub, pour que les messages soient persistés.
        """
        r = redis_lib.from_url(REDIS_URL, decode_responses=True)

        raw_listening = r.lrange("listening_events_queue", 0, -1)
        r.delete("listening_events_queue")
        raw_p2p = r.lrange("p2p_network_events_queue", 0, -1)
        r.delete("p2p_network_events_queue")

        listening, p2p = [], []
        for msg in raw_listening:
            try:
                listening.append(json.loads(msg))
            except json.JSONDecodeError:
                pass
        for msg in raw_p2p:
            try:
                p2p.append(json.loads(msg))
            except json.JSONDecodeError:
                pass

        logger.info("consume_from_redis: %d listening, %d p2p", len(listening), len(p2p))
        return {"listening": listening, "p2p_network": p2p}

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les champs obligatoires et envoie les invalides en DLQ.
        Sépare les listening_events des p2p_network_events.
        """
        valid_listening, valid_p2p, errors = [], [], []

        for event in raw_events.get("listening", []):
            missing = LISTENING_REQUIRED - event.keys()
            invalid_duration = not isinstance(event.get("duration_ms"), (int, float)) or event.get("duration_ms", 0) <= 0
            if missing or invalid_duration:
                errors.append(("redis_listening_events", event, "validation", f"missing={missing}"))
            else:
                valid_listening.append(event)

        for event in raw_events.get("p2p_network", []):
            missing = P2P_REQUIRED - event.keys()
            if missing:
                errors.append(("p2p_network_events", event, "validation", f"missing={missing}"))
            else:
                valid_p2p.append(event)

        if errors:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """INSERT INTO dead_letter_events (id, original_topic, payload, error_type, error_message)
                           VALUES (%s, %s, %s, %s, %s)""",
                        [(str(uuid.uuid4()), topic, json.dumps(ev), etype, emsg)
                         for topic, ev, etype, emsg in errors],
                    )
                conn.commit()

        logger.info("validate_events: %d valid listening, %d valid p2p, %d errors",
                    len(valid_listening), len(valid_p2p), len(errors))
        return {"valid_listening": valid_listening, "valid_p2p": valid_p2p, "errors": len(errors)}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les listening_events avec les données du catalogue PostgreSQL.
        Batch query par track_id pour éviter le N+1.
        Les track_id inconnus passent sans enrichissement (catalogue peut ne pas être chargé).
        """
        events = validated.get("valid_listening", [])
        if not events:
            return []

        track_ids = list({e["track_id"] for e in events})
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%s::uuid[])",
                    (track_ids,)
                )
                rows = cur.fetchall()

        track_map = {
            str(row[0]): {"title": row[1], "artist_id": str(row[2]) if row[2] else None, "genre": row[3]}
            for row in rows
        }

        enriched = []
        unknown = 0
        for event in events:
            info = track_map.get(event["track_id"])
            if not info:
                unknown += 1
            enriched.append({
                **event,
                "track_title": info["title"] if info else None,
                "artist_id":   info["artist_id"] if info else None,
                "genre":       info["genre"] if info else event.get("genre"),
            })

        logger.info("enrich_events: %d enriched, %d unknown track_ids", len(enriched), unknown)
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Écrit les événements enrichis en Parquet sur MinIO.
        Partitionnement par date + heure pour la parallélisation Phase 2.
        """
        if not enriched_events:
            logger.info("store_to_parquet: no events")
            return ""

        df = pd.DataFrame(enriched_events)
        df["_ts"] = pd.to_datetime(df["timestamp"], utc=True)
        df["_date"] = df["_ts"].dt.strftime("%Y-%m-%d")
        df["_hour"] = df["_ts"].dt.hour

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        run_id = context["run_id"].replace(":", "-").replace("+", "-")[:40]
        paths = []

        for (date, hour), group in df.groupby(["_date", "_hour"]):
            group = group.drop(columns=["_ts", "_date", "_hour"])
            table = pa.Table.from_pandas(group, preserve_index=False)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            key = f"listening_events/date={date}/hour={hour:02d}/part-{run_id}.parquet"
            s3.put_object(Bucket="spotify-parquet", Key=key, Body=buf.getvalue())
            paths.append(key)

        logger.info("store_to_parquet: %d files written", len(paths))
        return ", ".join(paths)

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les événements dans listening_events de façon idempotente.
        ON CONFLICT (id) DO NOTHING garantit la ré-exécution sans doublons.
        Les FK violations (track_id inconnu) sont ignorées silencieusement.
        """
        if not enriched_events:
            return {"inserted": 0, "skipped": 0}

        rows = [
            (
                e.get("event_id", str(uuid.uuid4())),
                e["user_id"],
                e["track_id"],
                e.get("source_peer"),
                e["timestamp"],
                int(e.get("duration_ms", 0)),
                e.get("device_type"),
                e.get("geo_country"),
                bool(e.get("completed", False)),
                e.get("event_source", "p2p"),
            )
            for e in enriched_events
        ]

        sql = """
            INSERT INTO listening_events
                (id, user_id, track_id, source_peer_id, timestamp, duration_ms,
                 device_type, geo_country, completed, event_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        inserted = 0
        skipped = 0

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    try:
                        cur.execute(sql, row)
                        if cur.rowcount > 0:
                            inserted += 1
                        else:
                            skipped += 1
                    except Exception as e:
                        # FK violation si track_id absent du catalogue
                        conn.rollback()
                        skipped += 1
                        logger.debug("upsert skipped event %s: %s", row[0], e)
                        # re-ouvrir la transaction après rollback
                        cur = conn.cursor()
            conn.commit()

        logger.info("upsert_to_postgres: %d inserted, %d skipped", inserted, skipped)
        return {"inserted": inserted, "skipped": skipped}

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
