"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()
        → load_to_postgres()
        → notify_success()
"""

import json
import logging
import uuid
from datetime import datetime, timedelta

import boto3
import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

DEFAULT_ARGS = {
    "owner":                     "spotify-team",
    "depends_on_past":           False,
    "start_date":                datetime(2025, 1, 1),
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_CONN_ID    = "spotify_minio"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]

ARTIST_REQUIRED  = {"id", "name", "label"}
ALBUM_REQUIRED   = {"id", "artist_id", "title"}
TRACK_REQUIRED   = {"id", "artist_id", "title", "duration_ms"}


def _get_pg_conn():
    try:
        conn_info = BaseHook.get_connection(POSTGRES_CONN_ID)
        return psycopg2.connect(
            host=conn_info.host,
            port=conn_info.port or 5432,
            dbname=conn_info.schema,
            user=conn_info.login,
            password=conn_info.password,
        )
    except Exception as e:
        log.error("Connexion PostgreSQL échouée : %s", e)
        return None


def _get_s3_client():
    try:
        conn_info = BaseHook.get_connection(MINIO_CONN_ID)
        extra = conn_info.extra_dejson
        endpoint = extra.get("endpoint_url", "http://minio:9000")
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=conn_info.login or "minioadmin",
            aws_secret_access_key=conn_info.password or "minioadmin",
        )
    except Exception:
        return boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )


with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list:
        """Télécharge les 3 JSONs depuis MinIO et retourne les catalogues bruts."""
        s3 = _get_s3_client()
        catalogs = []
        for filename in LABEL_FILES:
            try:
                response = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(response["Body"].read().decode("utf-8"))
                catalogs.append(catalog)
                log.info("Téléchargé : %s (%d artistes)", filename, len(catalog.get("artists", [])))
            except Exception as e:
                log.warning("Fichier manquant ou erreur : %s — %s", filename, e)
        log.info("Total catalogues extraits : %d", len(catalogs))
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list) -> dict:
        """
        Valide les champs obligatoires de chaque entité.
        Les invalides partent en DLQ.
        """
        valid = {"artists": [], "albums": [], "tracks": []}
        errors_count = 0
        dlq_rows = []

        for catalog in raw_catalogs:
            for artist in catalog.get("artists", []):
                if ARTIST_REQUIRED.issubset(artist.keys()):
                    valid["artists"].append(artist)
                else:
                    errors_count += 1
                    dlq_rows.append(("catalog_ingestion", json.dumps(artist), "schema_validation", "Champs manquants artiste"))

            for album in catalog.get("albums", []):
                if ALBUM_REQUIRED.issubset(album.keys()):
                    valid["albums"].append(album)
                else:
                    errors_count += 1
                    dlq_rows.append(("catalog_ingestion", json.dumps(album), "schema_validation", "Champs manquants album"))

            for track in catalog.get("tracks", []):
                if TRACK_REQUIRED.issubset(track.keys()):
                    valid["tracks"].append(track)
                else:
                    errors_count += 1
                    dlq_rows.append(("catalog_ingestion", json.dumps(track), "schema_validation", "Champs manquants track"))

        # Insérer les invalides en DLQ
        if dlq_rows:
            conn = _get_pg_conn()
            if conn:
                try:
                    with conn:
                        with conn.cursor() as cur:
                            cur.executemany(
                                """INSERT INTO dead_letter_events
                                   (original_topic, payload, error_type, error_message)
                                   VALUES (%s, %s::jsonb, %s, %s)""",
                                dlq_rows
                            )
                    log.info("DLQ : %d entrées invalides insérées", len(dlq_rows))
                finally:
                    conn.close()

        log.info("Validation : %d artistes, %d albums, %d tracks valides | %d erreurs",
                 len(valid["artists"]), len(valid["albums"]), len(valid["tracks"]), errors_count)
        return {"valid": valid, "errors_count": errors_count}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Normalise les données :
        - Noms d'artistes : strip + title case
        - Durées : 0 < duration_ms < 3_600_000
        - Dédoublonnage par id
        """
        data = validated["valid"]

        # Normaliser artistes
        seen_artists = {}
        for artist in data["artists"]:
            artist["name"] = artist["name"].strip().title()
            aid = artist["id"]
            if aid not in seen_artists:
                seen_artists[aid] = artist

        # Filtrer tracks avec durée valide
        valid_tracks = [
            t for t in data["tracks"]
            if 0 < t.get("duration_ms", 0) < 3_600_000
        ]

        # Dédoublonnage albums par id
        seen_albums = {a["id"]: a for a in data["albums"]}

        result = {
            "artists": list(seen_artists.values()),
            "albums":  list(seen_albums.values()),
            "tracks":  valid_tracks,
            "errors_count": validated["errors_count"],
        }
        log.info("Transform : %d artistes, %d albums, %d tracks",
                 len(result["artists"]), len(result["albums"]), len(result["tracks"]))
        return result

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Upsert idempotent dans artists, albums, tracks.
        """
        conn = _get_pg_conn()
        if conn is None:
            log.error("Pas de connexion PostgreSQL")
            return {"artists_inserted": 0, "albums_inserted": 0, "tracks_inserted": 0, "errors_count": 0}

        artists_inserted = 0
        albums_inserted  = 0
        tracks_inserted  = 0

        try:
            with conn:
                with conn.cursor() as cur:

                    # ── Artists ──────────────────────────────
                    for a in transformed["artists"]:
                        cur.execute("""
                            INSERT INTO artists (id, name, country, label, genres, monthly_listeners, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (name, label) DO UPDATE SET
                                country           = EXCLUDED.country,
                                genres            = EXCLUDED.genres,
                                monthly_listeners = EXCLUDED.monthly_listeners,
                                updated_at        = NOW()
                            RETURNING id
                        """, (
                            a["id"], a["name"], a.get("country"), a.get("label"),
                            a.get("genres", []), a.get("monthly_listeners", 0)
                        ))
                        artists_inserted += 1

                    # ── Albums ───────────────────────────────
                    for alb in transformed["albums"]:
                        cur.execute("""
                            INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                title        = EXCLUDED.title,
                                release_year = EXCLUDED.release_year,
                                total_tracks = EXCLUDED.total_tracks
                        """, (
                            alb["id"], alb["artist_id"], alb["title"],
                            alb.get("release_year"), alb.get("total_tracks")
                        ))
                        albums_inserted += 1

                    # ── Tracks ───────────────────────────────
                    for t in transformed["tracks"]:
                        cur.execute("""
                            INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (id) DO UPDATE SET
                                title           = EXCLUDED.title,
                                duration_ms     = EXCLUDED.duration_ms,
                                genre           = EXCLUDED.genre,
                                updated_at      = NOW()
                        """, (
                            t["id"], t.get("album_id"), t["artist_id"], t["title"],
                            t["duration_ms"], t.get("genre"), t.get("bpm"),
                            t.get("explicit", False), t.get("audio_file_path")
                        ))
                        tracks_inserted += 1

            log.info("Load : %d artistes, %d albums, %d tracks insérés/mis à jour",
                     artists_inserted, albums_inserted, tracks_inserted)

        finally:
            conn.close()

        stats = {
            "artists_inserted": artists_inserted,
            "albums_inserted":  albums_inserted,
            "tracks_inserted":  tracks_inserted,
            "errors_count":     transformed.get("errors_count", 0),
        }

        context["ti"].xcom_push(key="tracks_inserted", value=tracks_inserted)
        context["ti"].xcom_push(key="errors_count",    value=transformed.get("errors_count", 0))
        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        dag_run = context["dag_run"]
        log.info("""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun          : %s
        Tracks insérées : %d
        Artists insérés : %d
        Albums insérés  : %d
        Erreurs DLQ     : %d
        """,
        dag_run.run_id,
        stats.get("tracks_inserted", 0),
        stats.get("artists_inserted", 0),
        stats.get("albums_inserted", 0),
        stats.get("errors_count", 0))

    # ── Orchestration ─────────────────────────────────────────
    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)
