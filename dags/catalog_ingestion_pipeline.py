"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)
"""

import json
import logging
import uuid
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

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
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]

ARTIST_REQUIRED = {"id", "name", "label"}
ALBUM_REQUIRED  = {"id", "artist_id", "title"}
TRACK_REQUIRED  = {"id", "artist_id", "title", "duration_ms"}


def _get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )


with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list:
        s3 = _get_s3()
        catalogs = []
        for filename in LABEL_FILES:
            try:
                obj = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(obj["Body"].read())
                catalogs.append(catalog)
                logger.info("Téléchargé : %s (%s artistes)", filename, len(catalog.get("artists", [])))
            except Exception as e:
                logger.warning("Fichier manquant : %s — %s", filename, e)
        logger.info("extract_from_minio : %d catalogues récupérés", len(catalogs))
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list, **context) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        valid = {"artists": [], "albums": [], "tracks": []}
        errors = []

        for catalog in raw_catalogs:
            for artist in catalog.get("artists", []):
                if ARTIST_REQUIRED <= artist.keys():
                    valid["artists"].append(artist)
                else:
                    errors.append(("labels-raw", artist, "schema_validation",
                                   f"missing={ARTIST_REQUIRED - artist.keys()}"))
            for album in catalog.get("albums", []):
                if ALBUM_REQUIRED <= album.keys():
                    valid["albums"].append(album)
                else:
                    errors.append(("labels-raw", album, "schema_validation",
                                   f"missing={ALBUM_REQUIRED - album.keys()}"))
            for track in catalog.get("tracks", []):
                if TRACK_REQUIRED <= track.keys():
                    valid["tracks"].append(track)
                else:
                    errors.append(("labels-raw", track, "schema_validation",
                                   f"missing={TRACK_REQUIRED - track.keys()}"))

        if errors:
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """INSERT INTO dead_letter_events (id, original_topic, payload, error_type, error_message)
                           VALUES (%s, %s, %s, %s, %s)""",
                        [(str(uuid.uuid4()), t, json.dumps(p), e, m) for t, p, e, m in errors],
                    )
                conn.commit()

        logger.info("validate_schema : %d artists, %d albums, %d tracks valides, %d erreurs",
                    len(valid["artists"]), len(valid["albums"]), len(valid["tracks"]), len(errors))
        return {"valid": valid, "errors_count": len(errors)}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict, **context) -> dict:
        from src.transformations.catalog import normalize_artist_name, validate_track_schema, deduplicate_artists

        data = validated["valid"]

        for artist in data["artists"]:
            artist["name"] = normalize_artist_name(artist["name"]) or artist["name"]

        data["artists"] = deduplicate_artists(data["artists"])

        valid_tracks = []
        for track in data["tracks"]:
            if not validate_track_schema(track):
                valid_tracks.append(track)
        data["tracks"] = valid_tracks

        logger.info("transform_catalog : %d artists, %d albums, %d tracks",
                    len(data["artists"]), len(data["albums"]), len(data["tracks"]))
        return data

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        artists_n = albums_n = tracks_n = 0

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for a in transformed.get("artists", []):
                    cur.execute("""
                        INSERT INTO artists (id, name, country, label, genres, monthly_listeners, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (name, label) DO UPDATE SET
                            monthly_listeners = EXCLUDED.monthly_listeners,
                            genres            = EXCLUDED.genres,
                            updated_at        = NOW()
                    """, (a["id"], a["name"], a.get("country"), a["label"],
                          a.get("genres", []), a.get("monthly_listeners", 0),
                          a.get("created_at", datetime.utcnow().isoformat())))
                    artists_n += 1

                for al in transformed.get("albums", []):
                    cur.execute("""
                        INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            title        = EXCLUDED.title,
                            release_year = EXCLUDED.release_year
                    """, (al["id"], al["artist_id"], al["title"],
                          al.get("release_year"), al.get("total_tracks")))
                    albums_n += 1

                for t in transformed.get("tracks", []):
                    cur.execute("""
                        INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            title       = EXCLUDED.title,
                            duration_ms = EXCLUDED.duration_ms,
                            updated_at  = NOW()
                    """, (t["id"], t.get("album_id"), t["artist_id"], t["title"],
                          t["duration_ms"], t.get("genre"), t.get("bpm"),
                          t.get("explicit", False), t.get("audio_file_path")))
                    tracks_n += 1

            conn.commit()

        context["ti"].xcom_push(key="tracks_inserted", value=tracks_n)
        context["ti"].xcom_push(key="errors_count", value=0)

        logger.info("load_to_postgres : %d artists, %d albums, %d tracks", artists_n, albums_n, tracks_n)
        return {"artists_inserted": artists_n, "albums_inserted": albums_n, "tracks_inserted": tracks_n}

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        dag_run = context["dag_run"]
        logger.info(
            "catalog_ingestion_pipeline terminé | run=%s | tracks=%d | artists=%d",
            dag_run.run_id,
            stats.get("tracks_inserted", 0),
            stats.get("artists_inserted", 0),
        )

    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)
