"""Storage abstraction layer for pre-computed F1 data.

Supports two backends:
- local: reads/writes JSON files to a local directory (default)
- r2: reads/writes to Cloudflare R2 (S3-compatible)

Set STORAGE_MODE=r2 to use R2, otherwise defaults to local.
Set DATA_DIR to control the local storage directory (default: ./data).
"""

from __future__ import annotations

import gzip
import json
import logging
import os
from pathlib import Path
from functools import lru_cache

logger = logging.getLogger(__name__)


def _mode() -> str:
    return os.environ.get("STORAGE_MODE", "local").lower()


# ---------------------------------------------------------------------------
# Local filesystem backend
# ---------------------------------------------------------------------------

def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data")))


# ---------------------------------------------------------------------------
# Human-readable session folders (local backend only)
#
# The app addresses sessions by the numeric key "sessions/{year}/{round}/{type}".
# On the local filesystem we translate that to readable folders, e.g.
#     sessions/2024/01 - Bahrain Grand Prix/Race/replay.zst
# so the data is easy to browse. The translation lives here so the rest of the
# app keeps using numeric keys. (R2 keeps the compact numeric keys.)
# ---------------------------------------------------------------------------

_SESSION_TYPE_LABEL = {
    "R": "Race", "Q": "Qualifying", "S": "Sprint", "SQ": "Sprint Qualifying",
    "FP1": "Practice 1", "FP2": "Practice 2", "FP3": "Practice 3",
}
_round_name_cache: dict[str, str] = {}  # "year/round" -> event name


def _sanitize_folder(name: str) -> str:
    import re
    name = re.sub(r'[<>:"/\\|?*]', "", str(name)).strip().strip(".")
    return re.sub(r"\s+", " ", name) or "Unknown"


def _event_name(year: str, rnd: str) -> str | None:
    key = f"{year}/{rnd}"
    if key not in _round_name_cache:
        try:  # read the schedule directly (not a session key, so no recursion)
            sp = _data_dir() / "seasons" / str(year) / "schedule.json"
            if sp.exists():
                raw = sp.read_bytes()
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                for ev in json.loads(raw).get("events", []):
                    if ev.get("round_number") is not None and ev.get("event_name"):
                        _round_name_cache[f"{year}/{ev['round_number']}"] = ev["event_name"]
        except Exception:
            pass
    return _round_name_cache.get(key)


def _round_dir_name(year: str, rnd: str) -> str:
    try:
        rn = int(rnd)
    except (TypeError, ValueError):
        return str(rnd)
    name = _event_name(year, rnd)
    return f"{rn:02d} - {_sanitize_folder(name)}" if name else f"{rn:02d}"


def _find_round_dir(ydir: Path, year: str, rnd: str) -> str:
    """Read-side: locate an existing round folder by number (readable or numeric)."""
    try:
        rn = int(rnd)
    except (TypeError, ValueError):
        return str(rnd)
    padded = f"{rn:02d}"
    if ydir.is_dir():
        for d in ydir.glob(f"{padded} - *"):
            if d.is_dir():
                return d.name
    for name in (padded, str(rn)):
        if (ydir / name).is_dir():
            return name
    return _round_dir_name(year, rnd)


def _find_type_dir(rdir: Path, stype: str) -> str:
    label = _SESSION_TYPE_LABEL.get(stype, stype)
    if (rdir / label).is_dir():
        return label
    if (rdir / stype).is_dir():
        return stype
    return label


def _resolve(path: str, for_read: bool) -> str:
    """Map a numeric session key to its readable on-disk relative path."""
    parts = path.split("/")
    if len(parts) < 4 or parts[0] != "sessions":
        return path
    year, rnd, stype = parts[1], parts[2], parts[3]
    rest = parts[4:]
    if for_read:
        ydir = _data_dir() / "sessions" / year
        round_dir = _find_round_dir(ydir, year, rnd)
        type_dir = _find_type_dir(ydir / round_dir, stype)
    else:
        round_dir = _round_dir_name(year, rnd)
        type_dir = _SESSION_TYPE_LABEL.get(stype, stype)
    return "/".join(["sessions", year, round_dir, type_dir, *rest])


def _fs(path: str, for_read: bool) -> Path:
    return _data_dir() / _resolve(path, for_read)


def _cache_name_from_info(path: str, data: object) -> None:
    """When info.json is written, remember its event name so the (first) folder
    write for that session uses the readable name."""
    parts = path.split("/")
    if (len(parts) == 5 and parts[0] == "sessions" and parts[4] == "info.json"
            and isinstance(data, dict) and data.get("event_name")):
        _round_name_cache[f"{parts[1]}/{parts[2]}"] = data["event_name"]


def _local_put_json(path: str, data: object) -> None:
    _cache_name_from_info(path, data)
    filepath = _fs(path, for_read=False)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    # Stored gzip-compressed (like the R2 backend). Read side auto-detects, so
    # existing uncompressed files keep working.
    body = gzip.compress(json.dumps(data, separators=(",", ":")).encode(), 6)
    filepath.write_bytes(body)
    logger.info(f"Saved {path} ({len(body)} bytes gzipped)")


def _local_get_json(path: str) -> object | None:
    filepath = _fs(path, for_read=True)
    if not filepath.exists():
        return None
    raw = filepath.read_bytes()
    if raw[:2] == b"\x1f\x8b":  # gzip magic
        raw = gzip.decompress(raw)
    return json.loads(raw)


def _local_put_bytes(path: str, data: bytes) -> None:
    filepath = _fs(path, for_read=False)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_bytes(data)
    logger.info(f"Saved {path} ({len(data)} bytes)")


def _local_get_bytes(path: str) -> bytes | None:
    filepath = _fs(path, for_read=True)
    if not filepath.exists():
        return None
    return filepath.read_bytes()


def _local_get_range(path: str, start: int, length: int) -> bytes | None:
    filepath = _fs(path, for_read=True)
    if not filepath.exists():
        return None
    with open(filepath, "rb") as fh:
        fh.seek(start)
        return fh.read(length)


def _local_exists(path: str) -> bool:
    return _fs(path, for_read=True).exists()


def _local_list_keys(prefix: str) -> list[str]:
    base = _data_dir() / prefix
    if not base.exists():
        return []
    return [str(p.relative_to(_data_dir())) for p in base.rglob("*") if p.is_file()]


# ---------------------------------------------------------------------------
# R2 backend
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _get_r2_client():
    import boto3
    from botocore.config import Config

    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    access_key = os.environ.get("R2_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY", "")

    if not all([account_id, access_key, secret_key]):
        raise RuntimeError("R2 credentials not configured. Set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY.")

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            region_name="auto",
            retries={"max_attempts": 3, "mode": "standard"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _r2_bucket() -> str:
    return os.environ.get("R2_BUCKET_NAME", "f1timingdata")


def _r2_key(path: str) -> str:
    return path.lstrip("/")


def _r2_put_json(path: str, data: object) -> None:
    client = _get_r2_client()
    body = gzip.compress(json.dumps(data, separators=(",", ":")).encode())
    client.put_object(
        Bucket=_r2_bucket(),
        Key=_r2_key(path),
        Body=body,
        ContentType="application/json",
        ContentEncoding="gzip",
    )
    logger.info(f"Uploaded {path} ({len(body)} bytes gzipped)")


def _r2_get_json(path: str) -> object | None:
    from botocore.exceptions import ClientError
    client = _get_r2_client()
    try:
        resp = client.get_object(Bucket=_r2_bucket(), Key=_r2_key(path))
        body = resp["Body"].read()
        try:
            body = gzip.decompress(body)
        except gzip.BadGzipFile:
            pass
        return json.loads(body)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def _r2_put_bytes(path: str, data: bytes) -> None:
    client = _get_r2_client()
    # Stored UNCOMPRESSED so byte-range reads address individual frames.
    client.put_object(
        Bucket=_r2_bucket(),
        Key=_r2_key(path),
        Body=data,
        ContentType="application/json",
    )
    logger.info(f"Uploaded {path} ({len(data)} bytes)")


def _r2_get_bytes(path: str) -> bytes | None:
    from botocore.exceptions import ClientError
    client = _get_r2_client()
    try:
        resp = client.get_object(Bucket=_r2_bucket(), Key=_r2_key(path))
        return resp["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def _r2_get_range(path: str, start: int, length: int) -> bytes | None:
    from botocore.exceptions import ClientError
    client = _get_r2_client()
    try:
        resp = client.get_object(
            Bucket=_r2_bucket(),
            Key=_r2_key(path),
            Range=f"bytes={start}-{start + length - 1}",
        )
        return resp["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def _r2_exists(path: str) -> bool:
    from botocore.exceptions import ClientError
    client = _get_r2_client()
    try:
        client.head_object(Bucket=_r2_bucket(), Key=_r2_key(path))
        return True
    except ClientError:
        return False


def _r2_list_keys(prefix: str) -> list[str]:
    client = _get_r2_client()
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=_r2_bucket(), Prefix=_r2_key(prefix)):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


# ---------------------------------------------------------------------------
# Public API - delegates to the configured backend
# ---------------------------------------------------------------------------

def put_json(path: str, data: object) -> None:
    if _mode() == "r2":
        _r2_put_json(path, data)
    else:
        _local_put_json(path, data)


def _github_fetch(path: str) -> bytes | None:
    """Fetch a small metadata file (schedule, pit_loss) from GITHUB_DATA_BASE_URL
    when it isn't local, and cache it locally. Session files are NOT fetched here
    (they come from per-session zips, handled in services.process)."""
    base = os.environ.get("GITHUB_DATA_BASE_URL", "").strip().rstrip("/")
    if not base or path.startswith("sessions/"):
        return None
    try:
        import httpx
        r = httpx.get(f"{base}/{path}", timeout=30, follow_redirects=True)
        if r.status_code != 200:
            return None
        _local_put_bytes(path, r.content)  # cache locally as-is
        return r.content
    except Exception:
        return None


def get_json(path: str) -> object | None:
    if _mode() == "r2":
        return _r2_get_json(path)
    v = _local_get_json(path)
    if v is None:
        raw = _github_fetch(path)
        if raw is not None:
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            try:
                return json.loads(raw)
            except Exception:
                return None
    return v


def put_bytes(path: str, data: bytes) -> None:
    if _mode() == "r2":
        _r2_put_bytes(path, data)
    else:
        _local_put_bytes(path, data)


def get_bytes(path: str) -> bytes | None:
    if _mode() == "r2":
        return _r2_get_bytes(path)
    v = _local_get_bytes(path)
    if v is None:
        v = _github_fetch(path)
    return v


def get_range(path: str, start: int, length: int) -> bytes | None:
    """Read `length` bytes starting at byte `start` from a stored object."""
    if _mode() == "r2":
        return _r2_get_range(path, start, length)
    return _local_get_range(path, start, length)


def is_local() -> bool:
    return _mode() != "r2"


def local_path(path: str) -> Path:
    """Absolute path of a stored object on the local filesystem (local mode only)."""
    return _fs(path, for_read=True)


def exists(path: str) -> bool:
    if _mode() == "r2":
        return _r2_exists(path)
    return _local_exists(path)


def list_keys(prefix: str) -> list[str]:
    if _mode() == "r2":
        return _r2_list_keys(prefix)
    return _local_list_keys(prefix)
