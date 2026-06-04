"""Fonctions de transformation du catalogue musical."""

from typing import Optional

REQUIRED_TRACK_FIELDS = {"id", "artist_id", "title", "duration_ms"}
MAX_DURATION_MS = 3_600_000  # 1 heure


def normalize_artist_name(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    return name.strip().title()


def validate_track_schema(track: dict) -> list:
    errors = []
    missing = REQUIRED_TRACK_FIELDS - track.keys()
    if missing:
        errors.append(f"missing fields: {missing}")
    duration = track.get("duration_ms")
    if duration is not None:
        if duration <= 0:
            errors.append(f"duration_ms must be > 0, got {duration}")
        elif duration > MAX_DURATION_MS:
            errors.append(f"duration_ms exceeds max ({MAX_DURATION_MS}ms), got {duration}")
    return errors


def deduplicate_artists(artists: list) -> list:
    seen = set()
    result = []
    for artist in artists:
        key = (normalize_artist_name(artist.get("name")), artist.get("label"))
        if key not in seen:
            seen.add(key)
            result.append(artist)
    return result
