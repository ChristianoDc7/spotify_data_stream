"""
SPOTIFY — Data Generator
========================
Génère des données fictives réalistes pour alimenter le catalogue SPOTIFY.
Utilise Faker pour créer artistes, albums et tracks crédibles.

Usage :
    python -m src.data_generator.generate_catalog --labels data/labels/ --output sql/catalog_seed.sql
    python -m src.data_generator.generate_catalog --format json --output data/labels/label_a.json
"""

import argparse
import json
import random
import uuid
from datetime import datetime, date
from datetime import timezone
from pathlib import Path

from faker import Faker

fake = Faker(["fr_FR", "en_US", "de_DE", "es_ES"])

GENRES = ["Pop", "Rock", "Hip-Hop", "Electronic", "Jazz", "R&B", "Folk", "Latin", "Metal", "Classical"]
DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP"]

# Les 3 labels sont distribués différemment par groupe (le formateur fournit les JSONs)
LABEL_NAMES = ["SunSet Records", "NightWave Music", "Urban Pulse"]


def generate_artist(label: str) -> dict:
    """Génère un artiste fictif."""
    genres = random.sample(GENRES, k=random.randint(1, 3))
    return {
        "id":                str(uuid.uuid4()),
        "name":              fake.name(),
        "country":           random.choice(COUNTRIES),
        "label":             label,
        "genres":            genres,
        "monthly_listeners": random.randint(1000, 5_000_000),
        "created_at":        fake.date_time_between(start_date="-5y").isoformat(),
    }


def generate_album(artist: dict) -> dict:
    """Génère un album pour un artiste."""
    n_tracks = random.randint(8, 16)
    return {
        "id":          str(uuid.uuid4()),
        "artist_id":   artist["id"],
        "title":       f"{fake.word().capitalize()} {fake.word().capitalize()}",
        "release_year": random.randint(2015, 2025),
        "total_tracks": n_tracks,
    }


def generate_track(album: dict, artist: dict, track_num: int) -> dict:
    """Génère un track pour un album."""
    genre = random.choice(artist["genres"])
    duration_ms = random.randint(120_000, 360_000)  # 2 à 6 minutes
    return {
        "id":              str(uuid.uuid4()),
        "album_id":        album["id"],
        "artist_id":       artist["id"],
        "title":           f"{fake.word().capitalize()} {fake.word().capitalize()} (feat. {fake.first_name()})" if random.random() < 0.2 else f"{fake.word().capitalize()} {fake.word().capitalize()}",
        "duration_ms":     duration_ms,
        "genre":           genre,
        "bpm":             random.randint(60, 180),
        "explicit":        random.random() < 0.15,
        "audio_file_path": f"s3://spotify-audio/{artist['id']}/{album['id']}/track_{track_num:02d}.mp3",
    }


def generate_label_catalog(label_name: str, n_artists: int = 10) -> dict:
    """
    Génère un catalogue complet pour un label.

    Retourne un dict avec artists, albums, tracks.
    C'est ce format que le DAG catalog_ingestion_pipeline devra ingérer.
    """
    artists = [generate_artist(label_name) for _ in range(n_artists)]
    albums = []
    tracks = []

    for artist in artists:
        n_albums = random.randint(1, 4)
        for _ in range(n_albums):
            album = generate_album(artist)
            albums.append(album)
            for i in range(album["total_tracks"]):
                track = generate_track(album, artist, i + 1)
                tracks.append(track)

    return {
        "label":      label_name,
        "generated":  datetime.now(timezone.utc).isoformat(),
        "stats": {
            "artists": len(artists),
            "albums":  len(albums),
            "tracks":  len(tracks),
        },
        "artists": artists,
        "albums":  albums,
        "tracks":  tracks,
    }


def save_as_json(catalog: dict, output_path: Path):
    """Sauvegarde le catalogue au format JSON (pour MinIO / ingestion)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
    print(f"Catalogue sauvegardé : {output_path} ({catalog['stats']})")


def upload_to_minio(output_dir: Path, endpoint: str = "http://localhost:9000"):
    """Upload les JSON générés dans le bucket labels-raw de MinIO."""
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    for json_file in output_dir.glob("*.json"):
        s3.upload_file(str(json_file), "labels-raw", json_file.name)
        print(f"Uploadé : {json_file.name} → s3://labels-raw/{json_file.name}")


def main():
    parser = argparse.ArgumentParser(description="SPOTIFY Catalog Generator")
    parser.add_argument("--artists", type=int, default=10, help="Artistes par label")
    parser.add_argument("--output",  type=str, default="data/labels", help="Dossier de sortie")
    parser.add_argument("--upload",  action="store_true", help="Uploader automatiquement sur MinIO")
    parser.add_argument("--minio",   type=str, default="http://localhost:9000", help="Endpoint MinIO")
    args = parser.parse_args()

    output_dir = Path(args.output)
    for label in LABEL_NAMES:
        catalog = generate_label_catalog(label, n_artists=args.artists)
        filename = label.lower().replace(" ", "_") + ".json"
        save_as_json(catalog, output_dir / filename)

    print(f"\n3 catalogues générés dans {output_dir}/")

    if args.upload:
        print("\nUpload vers MinIO...")
        upload_to_minio(output_dir, args.minio)
        print("Upload terminé. Lancer le DAG catalog_ingestion_pipeline.")


if __name__ == "__main__":
    main()
