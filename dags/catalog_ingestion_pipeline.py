"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG
# ─────────────────────────────────────────────────────────────

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

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT
# ─────────────────────────────────────────────────────────────

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

# Champs obligatoires par entité
REQUIRED_ARTIST_FIELDS = {"id", "name", "label"}
REQUIRED_ALBUM_FIELDS  = {"id", "artist_id", "title"}
REQUIRED_TRACK_FIELDS  = {"id", "artist_id", "title", "duration_ms"}

# Durée max d'un morceau (1 heure en ms)
MAX_DURATION_MS = 3_600_000

# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────────
    # TÂCHE 1 — EXTRACT
    # ──────────────────────────────────────────────────────────

    @task(task_id="extract_from_minio", retries=3, retry_delay=timedelta(minutes=5))
    def extract_from_minio(**context) -> list:
        """
        Télécharge les 3 fichiers JSON des labels depuis MinIO.

        Utilise boto3 avec l'endpoint MinIO (compatible S3).
        Si un fichier est manquant, on loggue un warning et on continue
        sans crash — on traite ce qu'on a.

        Returns:
            list[dict] : catalogues bruts, un dict par label
        """
        import boto3
        from botocore.exceptions import ClientError
        from airflow.hooks.base import BaseHook

        # Récupération des credentials depuis la connexion Airflow
        conn = BaseHook.get_connection(MINIO_CONN_ID)
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"http://{conn.host}:{conn.port or 9000}",
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
        )

        catalogs = []
        for filename in LABEL_FILES:
            try:
                response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=filename)
                content = response["Body"].read().decode("utf-8")
                catalog = json.loads(content)
                # On attache le nom de fichier source pour traçabilité
                catalog["_source_file"] = filename
                catalogs.append(catalog)
                log.info("✅ Téléchargé : %s (%d bytes)", filename, len(content))
            except ClientError as e:
                if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                    log.warning("⚠️  Fichier manquant dans MinIO : %s — ignoré", filename)
                else:
                    # Erreur réseau / auth → on laisse le retry gérer
                    raise
            except json.JSONDecodeError as e:
                log.error("❌ JSON invalide dans %s : %s", filename, e)
                # Fichier corrompu → on skip sans bloquer le pipeline

        log.info("Extract terminé : %d catalogue(s) récupéré(s)", len(catalogs))
        return catalogs

    # ──────────────────────────────────────────────────────────
    # TÂCHE 2 — VALIDATE
    # ──────────────────────────────────────────────────────────

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list, **context) -> dict:
        """
        Valide le schéma de chaque entrée et envoie les invalides en DLQ.

        Champs obligatoires :
          - artiste : id, name, label
          - album   : id, artist_id, title
          - track   : id, artist_id, title, duration_ms

        Les entrées invalides sont insérées dans dead_letter_events avec
        error_type='schema_validation'.

        Returns:
            dict : {"valid": {artists, albums, tracks}, "errors_count": N}
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        valid = {"artists": [], "albums": [], "tracks": []}
        errors = []
        errors_count = 0

        def _validate_entity(entity: dict, required_fields: set, entity_type: str, source: str):
            missing = required_fields - set(entity.keys())
            if missing:
                errors.append((
                    entity.get("id", "unknown"),
                    entity_type,
                    f"Champs manquants : {missing}",
                    "schema_validation",
                    source,
                    json.dumps(entity),
                ))
                return False
            # Vérification que les champs ne sont pas None ou vide
            for field in required_fields:
                if entity.get(field) is None or entity.get(field) == "":
                    errors.append((
                        entity.get("id", "unknown"),
                        entity_type,
                        f"Champ vide ou null : {field}",
                        "schema_validation",
                        source,
                        json.dumps(entity),
                    ))
                    return False
            return True

        for catalog in raw_catalogs:
            source = catalog.get("_source_file", "unknown")

            for artist in catalog.get("artists", []):
                if _validate_entity(artist, REQUIRED_ARTIST_FIELDS, "artist", source):
                    valid["artists"].append(artist)

            for album in catalog.get("albums", []):
                if _validate_entity(album, REQUIRED_ALBUM_FIELDS, "album", source):
                    valid["albums"].append(album)

            for track in catalog.get("tracks", []):
                if _validate_entity(track, REQUIRED_TRACK_FIELDS, "track", source):
                    valid["tracks"].append(track)

        # Insertion en DLQ
        if errors:
            cursor.executemany(
                """
                INSERT INTO dead_letter_events
                    (event_id, event_type, error_message, error_type, source, raw_payload, created_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (event_id) DO NOTHING
                """,
                errors,
            )
            conn.commit()
            errors_count = len(errors)
            log.warning("⚠️  %d entrée(s) envoyée(s) en DLQ", errors_count)

        cursor.close()
        conn.close()

        log.info(
            "Validation terminée — artists: %d, albums: %d, tracks: %d, erreurs: %d",
            len(valid["artists"]), len(valid["albums"]), len(valid["tracks"]), errors_count,
        )

        # On pousse errors_count dans XCom pour notify_success
        context["ti"].xcom_push(key="errors_count", value=errors_count)

        return {"valid": valid, "errors_count": errors_count}

    # ──────────────────────────────────────────────────────────
    # TÂCHE 3 — TRANSFORM
    # ──────────────────────────────────────────────────────────

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Normalise et dédoublonne les données du catalogue.

        Opérations :
          - Artistes : strip + title case sur name, dédoublonnage sur (name, label)
          - Tracks   : filtrage des durées invalides (≤ 0 ou > 1 heure)
          - Genres   : normalisation en minuscule + strip

        Returns:
            dict avec keys "artists", "albums", "tracks"
        """
        valid = validated["valid"]

        # ── Normalisation des artistes ────────────────────────
        seen_artists = {}
        artists_out = []
        for artist in valid["artists"]:
            name_normalized  = artist["name"].strip().title()
            label_normalized = artist["label"].strip()
            dedup_key = (name_normalized.lower(), label_normalized.lower())

            if dedup_key not in seen_artists:
                seen_artists[dedup_key] = True
                artists_out.append({
                    **artist,
                    "name":  name_normalized,
                    "label": label_normalized,
                    # Normalisation du genre si présent
                    "genre": _normalize_genre(artist.get("genre", "")),
                })
            else:
                log.debug("Doublon artiste ignoré : %s / %s", name_normalized, label_normalized)

        # ── Normalisation des albums ──────────────────────────
        seen_albums = set()
        albums_out = []
        for album in valid["albums"]:
            if album["id"] not in seen_albums:
                seen_albums.add(album["id"])
                albums_out.append({
                    **album,
                    "title": album["title"].strip(),
                })

        # ── Normalisation des tracks ──────────────────────────
        seen_tracks = set()
        tracks_out = []
        skipped_duration = 0
        for track in valid["tracks"]:
            duration = track.get("duration_ms", 0)
            if not (0 < duration < MAX_DURATION_MS):
                log.warning(
                    "Track '%s' ignorée — durée invalide : %d ms",
                    track.get("title"), duration,
                )
                skipped_duration += 1
                continue
            if track["id"] not in seen_tracks:
                seen_tracks.add(track["id"])
                tracks_out.append({
                    **track,
                    "title": track["title"].strip(),
                    "genre": _normalize_genre(track.get("genre", "")),
                })

        if skipped_duration:
            log.warning("%d track(s) écartée(s) pour durée invalide", skipped_duration)

        log.info(
            "Transform terminé — artists: %d, albums: %d, tracks: %d",
            len(artists_out), len(albums_out), len(tracks_out),
        )

        return {
            "artists": artists_out,
            "albums":  albums_out,
            "tracks":  tracks_out,
        }

    def _normalize_genre(genre: str) -> str:
        """Normalise un genre musical : minuscule, strip, valeur par défaut."""
        if not genre:
            return "unknown"
        return genre.strip().lower()

    # ──────────────────────────────────────────────────────────
    # TÂCHE 4 — LOAD
    # ──────────────────────────────────────────────────────────

    @task(task_id="load_to_postgres", retries=3, retry_delay=timedelta(minutes=2))
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge les données dans PostgreSQL avec upsert idempotent.

        Utilise ON CONFLICT DO UPDATE pour garantir l'idempotence :
        relancer le même DAGrun produit exactement le même état final.

        Returns:
            dict : stats {tracks_inserted, artists_inserted, albums_inserted}
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        artists_inserted = 0
        albums_inserted  = 0
        tracks_inserted  = 0

        try:
            # ── Upsert artists ────────────────────────────────
            artist_rows = [
                (
                    a["id"],
                    a["name"],
                    a["label"],
                    a.get("genre", "unknown"),
                    a.get("country", None),
                    a.get("bio", None),
                )
                for a in transformed["artists"]
            ]
            if artist_rows:
                cursor.executemany(
                    """
                    INSERT INTO artists (id, name, label, genre, country, bio, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (name, label) DO UPDATE SET
                        genre      = EXCLUDED.genre,
                        country    = EXCLUDED.country,
                        bio        = EXCLUDED.bio,
                        updated_at = NOW()
                    """,
                    artist_rows,
                )
                artists_inserted = cursor.rowcount
                log.info("Artists upsertés : %d", artists_inserted)

            # ── Upsert albums ─────────────────────────────────
            album_rows = [
                (
                    a["id"],
                    a["artist_id"],
                    a["title"],
                    a.get("release_year", None),
                    a.get("cover_url", None),
                )
                for a in transformed["albums"]
            ]
            if album_rows:
                cursor.executemany(
                    """
                    INSERT INTO albums (id, artist_id, title, release_year, cover_url, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        title        = EXCLUDED.title,
                        release_year = EXCLUDED.release_year,
                        cover_url    = EXCLUDED.cover_url,
                        updated_at   = NOW()
                    """,
                    album_rows,
                )
                albums_inserted = cursor.rowcount
                log.info("Albums upsertés : %d", albums_inserted)

            # ── Upsert tracks ─────────────────────────────────
            track_rows = [
                (
                    t["id"],
                    t["artist_id"],
                    t.get("album_id", None),
                    t["title"],
                    t["duration_ms"],
                    t.get("genre", "unknown"),
                    t.get("audio_url", None),
                    t.get("isrc", None),
                )
                for t in transformed["tracks"]
            ]
            if track_rows:
                cursor.executemany(
                    """
                    INSERT INTO tracks
                        (id, artist_id, album_id, title, duration_ms, genre, audio_url, isrc, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        title       = EXCLUDED.title,
                        duration_ms = EXCLUDED.duration_ms,
                        genre       = EXCLUDED.genre,
                        audio_url   = EXCLUDED.audio_url,
                        updated_at  = NOW()
                    """,
                    track_rows,
                )
                tracks_inserted = cursor.rowcount
                log.info("Tracks upsertées : %d", tracks_inserted)

            conn.commit()

        except Exception as e:
            conn.rollback()
            log.error("Erreur lors du chargement PostgreSQL : %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

        stats = {
            "artists_inserted": artists_inserted,
            "albums_inserted":  albums_inserted,
            "tracks_inserted":  tracks_inserted,
            "errors_count":     validated_errors_count(context),
        }

        # Push XCom pour monitoring externe
        context["ti"].xcom_push(key="tracks_inserted",  value=tracks_inserted)
        context["ti"].xcom_push(key="artists_inserted", value=artists_inserted)
        context["ti"].xcom_push(key="albums_inserted",  value=albums_inserted)

        return stats

    def validated_errors_count(context: dict) -> int:
        """Récupère errors_count depuis le XCom de validate_schema."""
        try:
            return context["ti"].xcom_pull(
                task_ids="validate_schema", key="errors_count"
            ) or 0
        except Exception:
            return 0

    # ──────────────────────────────────────────────────────────
    # TÂCHE 5 — NOTIFY
    # ──────────────────────────────────────────────────────────

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """
        Log de succès avec statistiques d'ingestion.
        """
        dag_run = context["dag_run"]
        log.info(
            "✅ catalog_ingestion_pipeline terminé\n"
            "   DAGRun          : %s\n"
            "   Tracks insérées : %d\n"
            "   Artists insérés : %d\n"
            "   Albums insérés  : %d\n"
            "   Erreurs DLQ     : %d",
            dag_run.run_id,
            stats.get("tracks_inserted", 0),
            stats.get("artists_inserted", 0),
            stats.get("albums_inserted", 0),
            stats.get("errors_count", 0),
        )

        # Alerte si taux d'erreur élevé (> 10%)
        total = (
            stats.get("tracks_inserted", 0)
            + stats.get("artists_inserted", 0)
            + stats.get("albums_inserted", 0)
            + stats.get("errors_count", 0)
        )
        if total > 0 and stats.get("errors_count", 0) / total > 0.10:
            log.warning(
                "⚠️  Taux d'erreur élevé : %d/%d entrées en DLQ",
                stats.get("errors_count"), total,
            )

    # ── Orchestration des tâches ──────────────────────────────
    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)