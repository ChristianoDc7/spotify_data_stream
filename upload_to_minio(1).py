"""
Upload des catalogues JSON vers MinIO — bucket `labels-raw`

Usage :
    python upload_to_minio.py

Prérequis :
    - MinIO qui tourne (docker-compose up)
    - Les 3 fichiers JSON déjà générés dans data/labels/
    - pip install boto3
"""

import boto3
from botocore.exceptions import ClientError, EndpointResolutionError
from pathlib import Path

MINIO_ENDPOINT    = "http://localhost:9000"
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"
BUCKET_NAME       = "labels-raw"
LABELS_DIR        = Path("data/labels")

FILES = [
    "sunset_records.json",
    "nightwave_music.json",
    "urban_pulse.json",
]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url         = MINIO_ENDPOINT,
        aws_access_key_id    = MINIO_ACCESS_KEY,
        aws_secret_access_key= MINIO_SECRET_KEY,
    )


def ensure_bucket(s3, bucket: str):
    """Crée le bucket s'il n'existe pas déjà."""
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"  Bucket '{bucket}' déjà existant.")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
            print(f"  Bucket '{bucket}' créé.")
        else:
            raise


def upload_catalogs():
    print("=== Upload des catalogues vers MinIO ===\n")

    try:
        s3 = get_s3_client()
        ensure_bucket(s3, BUCKET_NAME)
    except Exception as e:
        print(f"[ERREUR] Connexion MinIO impossible : {e}")
        print("→ Vérifiez que docker-compose est lancé (docker-compose up -d minio)")
        return

    for filename in FILES:
        local_path = LABELS_DIR / filename
        if not local_path.exists():
            print(f"  [SKIP] {filename} introuvable — lancez d'abord generate_catalog.py")
            continue

        try:
            s3.upload_file(str(local_path), BUCKET_NAME, filename)
            size_kb = local_path.stat().st_size // 1024
            print(f"  ✅ {filename} uploadé ({size_kb} KB) → s3://{BUCKET_NAME}/{filename}")
        except Exception as e:
            print(f"  ❌ Erreur upload {filename} : {e}")

    print(f"\n✅ Upload terminé. Vérifiez sur http://localhost:9001 (minioadmin / minioadmin)")


if __name__ == "__main__":
    upload_catalogs()
