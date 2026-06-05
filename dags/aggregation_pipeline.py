"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT track_id,
                           COUNT(*)                        AS total_streams,
                           COUNT(DISTINCT user_id)         AS unique_listeners,
                           SUM(duration_ms)               AS total_duration_ms,
                           ARRAY_AGG(DISTINCT geo_country) AS countries
                    FROM listening_events
                    WHERE DATE(timestamp) = %s AND completed = TRUE
                    GROUP BY track_id
                    ORDER BY total_streams DESC
                    LIMIT 50
                """, (date,))
                rows = cur.fetchall()

        result = [
            {"track_id": str(r[0]), "total_streams": r[1], "unique_listeners": r[2],
             "total_duration_ms": r[3], "countries": r[4], "date": str(date)}
            for r in rows
        ]
        logger.info("compute_top_tracks : %d tracks pour %s", len(result), date)
        return result

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT t.artist_id,
                           COUNT(*)                AS total_streams,
                           COUNT(DISTINCT le.user_id) AS unique_listeners,
                           (SELECT le2.track_id
                            FROM listening_events le2
                            JOIN tracks t2 ON le2.track_id = t2.id
                            WHERE t2.artist_id = t.artist_id AND DATE(le2.timestamp) = %s
                            GROUP BY le2.track_id ORDER BY COUNT(*) DESC LIMIT 1
                           ) AS top_track_id
                    FROM listening_events le
                    JOIN tracks t ON le.track_id = t.id
                    WHERE DATE(le.timestamp) = %s
                    GROUP BY t.artist_id
                """, (date, date))
                rows = cur.fetchall()

        result = [
            {"artist_id": str(r[0]), "total_streams": r[1],
             "unique_listeners": r[2], "top_track_id": str(r[3]) if r[3] else None,
             "date": str(date)}
            for r in rows
        ]
        logger.info("compute_artist_stats : %d artistes pour %s", len(result), date)
        return result

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE event_source = 'cache')::float / NULLIF(COUNT(*), 0) AS cache_hit_rate,
                        COUNT(DISTINCT user_id) AS active_users,
                        COUNT(DISTINCT source_peer_id) AS active_peers
                    FROM listening_events
                    WHERE DATE(timestamp) = %s
                """, (date,))
                row = cur.fetchone()

        metrics = {
            "date":           str(date),
            "cache_hit_rate": round(float(row[0] or 0), 4),
            "active_users":   row[1],
            "active_peers":   row[2],
        }
        logger.info("compute_p2p_metrics : %s", metrics)
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for row in top_tracks:
                    cur.execute("""
                        INSERT INTO daily_streams
                            (track_id, date, total_streams, unique_listeners, total_duration_ms, countries, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (track_id, date) DO UPDATE SET
                            total_streams    = EXCLUDED.total_streams,
                            unique_listeners = EXCLUDED.unique_listeners,
                            total_duration_ms = EXCLUDED.total_duration_ms,
                            countries        = EXCLUDED.countries,
                            updated_at       = NOW()
                    """, (row["track_id"], row["date"], row["total_streams"],
                          row["unique_listeners"], row["total_duration_ms"], row["countries"]))

                for row in artist_stats:
                    cur.execute("""
                        INSERT INTO artist_stats
                            (artist_id, date, total_streams, unique_listeners, top_track_id, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (artist_id, date) DO UPDATE SET
                            total_streams    = EXCLUDED.total_streams,
                            unique_listeners = EXCLUDED.unique_listeners,
                            top_track_id     = EXCLUDED.top_track_id,
                            updated_at       = NOW()
                    """, (row["artist_id"], row["date"], row["total_streams"],
                          row["unique_listeners"], row.get("top_track_id")))

            conn.commit()

        logger.info("update_aggregates : %d daily_streams, %d artist_stats upsertés",
                    len(top_tracks), len(artist_stats))
        return {"daily_streams": len(top_tracks), "artist_stats": len(artist_stats)}

    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
