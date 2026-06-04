"""Fonctions de validation et d'enrichissement des événements d'écoute."""

from datetime import datetime, timezone
from typing import Optional

REQUIRED_LISTENING_FIELDS = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}
MIN_VALID_DURATION_MS = 5_000  # durée < 5s → pattern bot


def is_valid_listening_event(event: dict) -> bool:
    # Champs obligatoires présents et non nuls
    if not all(event.get(f) for f in REQUIRED_LISTENING_FIELDS):
        return False

    # Timestamp parseable et non dans le futur
    try:
        ts_str = str(event["timestamp"]).replace("Z", "+00:00")
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts > datetime.now(timezone.utc):
            return False
    except (ValueError, AttributeError):
        return False

    # Pattern bot : durée trop courte
    if event.get("duration_ms", 0) < MIN_VALID_DURATION_MS:
        return False

    return True
