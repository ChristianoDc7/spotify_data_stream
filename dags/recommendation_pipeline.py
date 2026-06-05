"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.
"""

import json
import logging
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7
MIN_LISTENS      = 3


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        import pandas as pd
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT user_id::text, track_id::text, COUNT(*) AS play_count
                    FROM listening_events
                    WHERE timestamp >= NOW() - INTERVAL '%s days'
                      AND completed = TRUE
                    GROUP BY user_id, track_id
                """, (LOOKBACK_DAYS,))
                rows = cur.fetchall()

        if not rows:
            logger.info("Aucune donnée d'écoute disponible")
            return {"matrix": {}, "users": [], "tracks": []}

        df = pd.DataFrame(rows, columns=["user_id", "track_id", "play_count"])

        # Garder uniquement les users avec >= MIN_LISTENS tracks distincts
        active_users = df.groupby("user_id")["track_id"].nunique()
        active_users = active_users[active_users >= MIN_LISTENS].index.tolist()
        df = df[df["user_id"].isin(active_users)]

        # Construire matrice pivot
        matrix = df.pivot_table(index="user_id", columns="track_id", values="play_count", fill_value=0)
        users  = matrix.index.tolist()
        tracks = matrix.columns.tolist()

        logger.info("build_user_track_matrix : %d users actifs, %d tracks", len(users), len(tracks))
        return {
            "matrix": matrix.to_dict(orient="index"),
            "users":  users,
            "tracks": tracks,
        }

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity

        users  = matrix_data["users"]
        tracks = matrix_data["tracks"]
        matrix = matrix_data["matrix"]

        if not users or len(users) < 2:
            logger.info("Pas assez d'utilisateurs pour les recommandations")
            return {}

        # Reconstruire numpy array
        arr = np.array([[matrix[u].get(t, 0) for t in tracks] for u in users], dtype=float)

        sim_matrix = cosine_similarity(arr)
        recommendations = {}

        for i, user_id in enumerate(users):
            # Tracks déjà écoutés par cet user
            listened = {t for j, t in enumerate(tracks) if arr[i][j] > 0}

            # Top voisins (excluant lui-même)
            similarities = [(sim_matrix[i][j], j) for j in range(len(users)) if j != i]
            similarities.sort(reverse=True)
            top_neighbors = [j for _, j in similarities[:10]]

            # Scores des tracks recommandés
            scores = {}
            for j in top_neighbors:
                weight = sim_matrix[i][j]
                for k, track in enumerate(tracks):
                    if track not in listened and arr[j][k] > 0:
                        scores[track] = scores.get(track, 0) + weight * arr[j][k]

            top_tracks = sorted(scores, key=scores.get, reverse=True)[:TOP_N_RECO]
            if top_tracks:
                recommendations[user_id] = top_tracks

        logger.info("compute_recommendations : %d users avec recommandations", len(recommendations))
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        import redis as redis_lib

        if not recommendations:
            return {"users_with_recos": 0, "total_recommendations": 0}

        r = redis_lib.from_url(REDIS_URL, decode_responses=True)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        total = 0

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for user_id, track_ids in recommendations.items():
                    # Redis
                    r.setex(f"reco:{user_id}", RECO_TTL_SECONDS, json.dumps(track_ids))

                    # PostgreSQL
                    score = 1.0
                    for track_id in track_ids:
                        cur.execute("""
                            INSERT INTO recommendations (user_id, track_id, score, generated_at)
                            VALUES (%s, %s, %s, NOW())
                            ON CONFLICT (user_id, track_id) DO UPDATE SET
                                score        = EXCLUDED.score,
                                generated_at = NOW()
                        """, (user_id, track_id, score))
                        score -= 0.05
                    total += len(track_ids)

            conn.commit()

        logger.info("store_recommendations : %d users, %d recommandations", len(recommendations), total)
        return {"users_with_recos": len(recommendations), "total_recommendations": total}

    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)
