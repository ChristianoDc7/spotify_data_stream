"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned
"""

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100

LISTENING_REQUIRED = {"user_id", "track_id", "timestamp", "duration_ms"}


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """Récupère les événements pending avec retry_count < MAX_RETRIES."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, payload, error_type, retry_count, original_topic
                    FROM dead_letter_events
                    WHERE status = 'pending'
                      AND retry_count < %s
                    ORDER BY created_at ASC
                    LIMIT %s
                """, (MAX_RETRIES, BATCH_SIZE))
                rows = cur.fetchall()

        events = [
            {
                "id":             str(row[0]),
                "payload":        row[1],
                "error_type":     row[2],
                "retry_count":    row[3],
                "original_topic": row[4],
            }
            for row in rows
        ]

        logger.info("fetch_pending_dlq: %d événements pending trouvés", len(events))
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger chaque événement défectueux.

        Règles de correction :
        - timestamp invalide/manquant → fallback sur now()
        - duration_ms manquant → fallback sur 0
        - user_id manquant → impossible à corriger → failed
        - track_id manquant → impossible à corriger → failed
        """
        reprocessed = []
        failed = []

        for event in pending_events:
            dlq_id = event["id"]
            retry_count = event["retry_count"]

            try:
                payload = event["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)

                corrected = dict(payload)

                # Correction timestamp
                if not corrected.get("timestamp"):
                    corrected["timestamp"] = datetime.now(timezone.utc).isoformat()

                # Correction duration_ms
                if not isinstance(corrected.get("duration_ms"), (int, float)) or corrected.get("duration_ms", 0) <= 0:
                    corrected["duration_ms"] = 0

                # Correction event_id
                if not corrected.get("event_id"):
                    corrected["event_id"] = str(uuid.uuid4())

                # Champs non corrigeables
                missing = LISTENING_REQUIRED - {k for k, v in corrected.items() if v}
                if missing:
                    failed.append({"id": dlq_id, "retry_count": retry_count, "reason": f"missing={missing}"})
                    continue

                # Seulement les listening_events sont réinjectés dans la table
                if event.get("original_topic") in ("listening_events", "redis_listening_events", None):
                    reprocessed.append({"id": dlq_id, "corrected": corrected})
                else:
                    # p2p_network_events : on marque reprocessed sans réinjection
                    reprocessed.append({"id": dlq_id, "corrected": None})

            except Exception as e:
                failed.append({"id": dlq_id, "retry_count": retry_count, "reason": str(e)})

        logger.info("reprocess_events: %d corrigés, %d échoués", len(reprocessed), len(failed))
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour dead_letter_events et réinjecte les events corrigés dans listening_events.
        """
        reprocessed = results.get("reprocessed", [])
        failed      = results.get("failed", [])

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        inserted = 0
        abandoned = 0
        still_pending = 0

        with hook.get_conn() as conn:
            with conn.cursor() as cur:

                # Events corrigés avec succès
                for item in reprocessed:
                    corrected = item.get("corrected")
                    if corrected:
                        try:
                            cur.execute("""
                                INSERT INTO listening_events
                                    (id, user_id, track_id, source_peer_id, timestamp,
                                     duration_ms, device_type, geo_country, completed, event_source)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id) DO NOTHING
                            """, (
                                corrected.get("event_id", str(uuid.uuid4())),
                                corrected.get("user_id"),
                                corrected.get("track_id"),
                                corrected.get("source_peer"),
                                corrected.get("timestamp"),
                                int(corrected.get("duration_ms", 0)),
                                corrected.get("device_type"),
                                corrected.get("geo_country"),
                                bool(corrected.get("completed", False)),
                                corrected.get("event_source", "p2p"),
                            ))
                            inserted += cur.rowcount
                        except Exception as e:
                            logger.warning("Réinjection échouée pour %s : %s", item["id"], e)
                            conn.rollback()

                    cur.execute("""
                        UPDATE dead_letter_events
                        SET status = 'reprocessed', resolved_at = NOW()
                        WHERE id = %s
                    """, (item["id"],))

                # Events toujours en échec
                for item in failed:
                    new_retry = item["retry_count"] + 1
                    new_status = "abandoned" if new_retry >= MAX_RETRIES else "pending"
                    cur.execute("""
                        UPDATE dead_letter_events
                        SET retry_count   = %s,
                            last_retry_at = NOW(),
                            status        = %s
                        WHERE id = %s
                    """, (new_retry, new_status, item["id"]))

                    if new_status == "abandoned":
                        abandoned += 1
                    else:
                        still_pending += 1

            conn.commit()

        logger.info("update_dlq_status: %d réinjectés, %d abandonnés, %d encore pending",
                    inserted, abandoned, still_pending)
        return {"reinjected": inserted, "abandoned": abandoned, "still_pending": still_pending}

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
