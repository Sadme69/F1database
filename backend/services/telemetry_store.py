"""Per-driver telemetry storage with zstd compression.

Telemetry is consumed whole-file per driver (the endpoint loads a driver's file
then picks a lap), so unlike replay frames it doesn't need per-record seeking —
it just needs good whole-file compression. zstd at a high level beats gzip by
~25-35% on the rounded telemetry arrays, with negligible decode cost.

Files are stored as ``{base}/telemetry/{driver}.zst``. A legacy fallback reads
the older gzipped ``{driver}.json`` so existing data keeps working.
"""

from __future__ import annotations

import json
import os

import zstandard as zstd

from services import storage

# Telemetry files are few (one per driver) and small, so a high level is cheap.
TEL_ZSTD_LEVEL = int(os.environ.get("TELEMETRY_ZSTD_LEVEL", "19"))


def put(base: str, driver: str, data: dict) -> None:
    """Compress and store one driver's telemetry as {base}/telemetry/{driver}.zst."""
    blob = zstd.ZstdCompressor(level=TEL_ZSTD_LEVEL).compress(
        json.dumps(data, separators=(",", ":")).encode()
    )
    storage.put_bytes(f"{base}/telemetry/{driver}.zst", blob)


def get(base: str, driver: str) -> dict | None:
    """Return a driver's telemetry dict, or None. Falls back to legacy gzipped JSON."""
    raw = storage.get_bytes(f"{base}/telemetry/{driver}.zst")
    if raw is not None:
        return json.loads(zstd.ZstdDecompressor().decompress(raw))
    # Legacy: telemetry stored as gzipped JSON before the zstd change.
    return storage.get_json(f"{base}/telemetry/{driver}.json")


def exists(base: str, driver: str) -> bool:
    return (
        storage.exists(f"{base}/telemetry/{driver}.zst")
        or storage.exists(f"{base}/telemetry/{driver}.json")
    )
