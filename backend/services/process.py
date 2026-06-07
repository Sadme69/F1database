"""On-demand session processing.

Shared by both the CLI precompute script and the backend's on-demand processing.
Uses locks to prevent duplicate processing of the same session.
"""

from __future__ import annotations

import asyncio
import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor

from services import storage, replay_store, telemetry_store

# NOTE: services.f1_data (which imports fastf1/pandas/numpy, ~85MB resident) is
# imported lazily inside process_session_sync so that serve-only deployments —
# which only read precomputed data — never pay that memory/startup cost.

logger = logging.getLogger(__name__)

# Locks to prevent duplicate processing of the same session
_locks: dict[str, asyncio.Lock] = {}


def _truthy(v: str) -> bool:
    return v.strip().lower() in ("1", "true", "yes", "on")


def _close_fastf1_requests_session(fastf1) -> None:
    """Close FastF1's cached requests session so its sqlite file is unlocked.

    clear_cache(deep=True) does os.remove() on fastf1_http_cache.sqlite, which
    fails while the file is still open (e.g. on Windows). Closing the session
    (and its sqlite backend) releases the handle; the session is recreated by the
    subsequent enable_cache().
    """
    for attr in ("_requests_session_cached", "_requests_session"):
        sess = getattr(fastf1.Cache, attr, None)
        if sess is None:
            continue
        for closer in (getattr(sess, "cache", None), sess):
            if closer is not None and hasattr(closer, "close"):
                try:
                    closer.close()
                except Exception:
                    pass
        try:
            setattr(fastf1.Cache, attr, None)
        except Exception:
            pass


def _maybe_clear_fastf1_cache(prefix: str, telemetry_mode: str) -> None:
    """Once a session is fully processed + compressed, the raw FastF1 cache is
    dead weight — the app serves only from the compressed files. Delete it so it
    never accumulates. Skipped for lazy telemetry, which still needs the cache to
    build charts on demand later.
    """
    if not _truthy(os.environ.get("DELETE_FASTF1_CACHE", "true")):
        return
    if telemetry_mode == "lazy":
        logger.warning(
            f"[{prefix}] DELETE_FASTF1_CACHE is on but TELEMETRY_MODE=lazy — keeping "
            "cache (lazy telemetry needs it). Use TELEMETRY_MODE=eager to allow deletion."
        )
        return
    try:
        import glob
        import shutil
        import fastf1
        from services import f1_data
        cache_dir = f1_data.CACHE_DIR
        # Serialize with session loads so we never clear mid-download, and free
        # the in-memory sessions (~1GB) at the same time.
        with f1_data._session_lock:
            f1_data._session_cache.clear()
            _close_fastf1_requests_session(fastf1)  # unlock the sqlite
            try:
                fastf1.Cache.clear_cache(cache_dir, deep=True)  # .ff1pkl + request sqlite
            except Exception as e:
                logger.warning(f"[{prefix}] clear_cache failed ({e}); manual cleanup")
                for entry in os.listdir(cache_dir):
                    p = os.path.join(cache_dir, entry)
                    if os.path.isdir(p):
                        shutil.rmtree(p, ignore_errors=True)
                for sq in glob.glob(os.path.join(cache_dir, "*.sqlite*")):
                    try:
                        os.remove(sq)
                    except Exception:
                        pass
            fastf1.Cache.enable_cache(cache_dir)  # fresh session + sqlite for later pulls
        logger.info(f"[{prefix}] Cleared FastF1 cache — serving now 100% from compressed files")
    except Exception as e:
        logger.warning(f"[{prefix}] Could not clear FastF1 cache: {e}")


def fetch_prebuilt_session(year: int, round_num: int, session_type: str) -> bool:
    """Download a prebuilt session zip from GITHUB_DATA_BASE_URL and extract it.

    Lets a serving box pull ready-made compressed data (no FastF1 needed) for a
    race the moment it's requested. Returns True if the session is now present.
    The zip layout is sessions/{year}/{round}/{type}.zip (see scripts/pack_sessions.py).
    """
    base_url = os.environ.get("GITHUB_DATA_BASE_URL", "").strip().rstrip("/")
    if not base_url:
        return False
    bkey = f"sessions/{year}/{round_num}/{session_type}"
    if storage.exists(f"{bkey}/replay.meta.json"):
        return True
    url = f"{base_url}/sessions/{year}/{round_num}/{session_type}.zip"
    try:
        import io
        import zipfile
        import gzip as _gz
        import json as _json
        import httpx
        resp = httpx.get(url, timeout=120, follow_redirects=True)
        if resp.status_code != 200:
            logger.info(f"[{year} R{round_num} {session_type}] no prebuilt zip ({resp.status_code})")
            return False
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        # prime the readable folder name from info.json before writing files
        try:
            raw = zf.read("info.json")
            ib = _gz.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
            ev = _json.loads(ib).get("event_name")
            if ev:
                storage._round_name_cache[f"{year}/{round_num}"] = ev
        except Exception:
            pass
        for name in zf.namelist():
            if name.endswith("/"):
                continue
            storage.put_bytes(f"{bkey}/{name}", zf.read(name))
        ok = storage.exists(f"{bkey}/replay.meta.json")
        if ok:
            logger.info(f"[{year} R{round_num} {session_type}] fetched prebuilt zip ({len(resp.content)//1024} KB)")
        return ok
    except Exception as e:
        logger.warning(f"[{year} R{round_num} {session_type}] prebuilt fetch failed: {e}")
        return False


def process_session_sync(
    year: int,
    round_num: int,
    session_type: str,
    skip_existing: bool = False,
    on_status: callable = None,
) -> bool:
    """Process and upload all data for a single session. Returns True if successful.

    on_status: optional callback(message: str) called with progress updates.
    """
    prefix = f"{year} R{round_num} {session_type}"
    base = f"sessions/{year}/{round_num}/{session_type}"

    if skip_existing and storage.exists(f"{base}/replay.meta.json"):
        logger.info(f"[{prefix}] Already exists, skipping")
        return True

    # Heavy data stack imported only when we actually process (keeps the
    # serve-only memory baseline low — see module docstring).
    from services.f1_data import (
        _get_session_info_sync,
        _get_track_data_sync,
        _get_lap_data_sync,
        _get_race_results_sync,
        _get_driver_positions_by_time_sync,
        _get_driver_telemetry_all_laps_sync,
    )

    def status(msg: str):
        logger.info(f"[{prefix}] {msg}")
        if on_status:
            on_status(msg)

    status("Loading session data from F1 API...")

    # Session info
    try:
        info = _get_session_info_sync(year, round_num, session_type)
        storage.put_json(f"{base}/info.json", info)
    except Exception as e:
        logger.error(f"[{prefix}] Failed to get session info: {e}")
        return False

    status("Processing track data...")

    # Track data
    try:
        track = _get_track_data_sync(year, round_num, session_type)
        storage.put_json(f"{base}/track.json", track)
    except Exception as e:
        logger.warning(f"[{prefix}] No track data: {e}")

    status("Processing lap data...")

    # Lap data
    laps = None
    try:
        laps = _get_lap_data_sync(year, round_num, session_type)
        storage.put_json(f"{base}/laps.json", laps)
    except Exception as e:
        logger.warning(f"[{prefix}] No lap data: {e}")

    # Results
    try:
        results = _get_race_results_sync(year, round_num, session_type)
        storage.put_json(f"{base}/results.json", results)
    except Exception as e:
        logger.warning(f"[{prefix}] No results: {e}")

    status("Building replay frames (this may take a minute)...")

    # Replay frames (the big one) — stored as per-frame zstd-compressed records
    # + offset index so serving seeks/decodes one frame at a time (~18x smaller
    # on disk, flat memory) instead of parsing the whole array into RAM.
    try:
        frames = _get_driver_positions_by_time_sync(year, round_num, session_type)
        replay_store.store_frames(base, frames)
        logger.info(f"[{prefix}] Stored {len(frames)} replay frames")
        del frames  # release ~hundreds of MB before telemetry export
    except Exception as e:
        logger.warning(f"[{prefix}] No replay data: {e}")

    # Telemetry build mode:
    #   eager (default) — pre-build all drivers now so the compressed data is
    #                     complete and the FastF1 cache can be deleted afterward.
    #   lazy            — skip; build per-driver on first request (keeps the cache).
    #   off             — never build telemetry (replay + track map only).
    telemetry_mode = os.environ.get("TELEMETRY_MODE", "eager").lower()

    if telemetry_mode == "eager":
        status("Processing telemetry...")
        # Drivers are independent, so export them in parallel. Each driver's
        # telemetry is built with ONE channel merge (then sliced per lap) — ~12x
        # faster than re-merging on every lap. FastF1's pandas/numpy work also
        # releases the GIL, so threading stacks on top of that.
        try:
            drivers = info.get("drivers", [])

            def export_driver(drv: dict) -> bool:
                abbr = drv["abbreviation"]
                try:
                    drv_telemetry = _get_driver_telemetry_all_laps_sync(
                        year, round_num, session_type, abbr
                    )
                except Exception:
                    drv_telemetry = {}
                if drv_telemetry:
                    telemetry_store.put(base, abbr, drv_telemetry)  # zstd
                    return True
                return False

            max_workers = min(len(drivers) or 1, int(os.environ.get("TELEMETRY_WORKERS", "8")))
            if max_workers > 1 and drivers:
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    list(pool.map(export_driver, drivers))
            else:
                for drv in drivers:
                    export_driver(drv)
            logger.info(f"[{prefix}] Uploaded telemetry for {len(drivers)} drivers ({max_workers} workers)")
        except Exception as e:
            logger.warning(f"[{prefix}] Telemetry upload issue: {e}")
    else:
        status(f"Skipping telemetry pre-build (TELEMETRY_MODE={telemetry_mode})")

    # Everything is now compressed — drop the raw FastF1 cache so it never grows.
    _maybe_clear_fastf1_cache(prefix, telemetry_mode)

    status("Processing complete")
    logger.info(f"[{prefix}] Done")
    return True


async def ensure_session_data(
    year: int,
    round_num: int,
    session_type: str,
    on_status: callable = None,
) -> bool:
    """Ensure session data exists, processing on-demand if needed.

    Uses per-session locks so concurrent requests wait rather than duplicate work.
    on_status: optional async callback(message: str) for progress updates.
    """
    base = f"sessions/{year}/{round_num}/{session_type}"

    # Fast path: data already exists
    if storage.exists(f"{base}/replay.meta.json"):
        return True

    # Get or create lock for this session
    key = f"{year}_{round_num}_{session_type}"
    if key not in _locks:
        _locks[key] = asyncio.Lock()

    async with _locks[key]:
        # Double-check after acquiring lock (another request may have finished)
        if storage.exists(f"{base}/replay.meta.json"):
            return True

        # Prefer a prebuilt zip from GitHub (fast, no FastF1) if configured.
        if await asyncio.to_thread(fetch_prebuilt_session, year, round_num, session_type):
            return True

        # Wrap sync callback for async on_status
        status_messages = []

        def sync_status(msg: str):
            status_messages.append(msg)

        # Run processing in a thread
        try:
            success = await asyncio.to_thread(
                process_session_sync,
                year,
                round_num,
                session_type,
                on_status=sync_status,
            )
            return success
        except Exception as e:
            logger.error(f"On-demand processing failed for {key}: {e}")
            traceback.print_exc()
            return False


async def ensure_session_data_ws(
    year: int,
    round_num: int,
    session_type: str,
    send_status,
) -> bool:
    """Like ensure_session_data but sends WebSocket status updates during processing."""
    base = f"sessions/{year}/{round_num}/{session_type}"

    if storage.exists(f"{base}/replay.meta.json"):
        return True

    key = f"{year}_{round_num}_{session_type}"
    if key not in _locks:
        _locks[key] = asyncio.Lock()

    # If another request is already processing, just wait
    if _locks[key].locked():
        await send_status("Waiting for session data (another request is processing)...")
        async with _locks[key]:
            return storage.exists(f"{base}/replay.meta.json")

    async with _locks[key]:
        if storage.exists(f"{base}/replay.meta.json"):
            return True

        # Prefer a prebuilt zip from GitHub (fast, no FastF1) if configured.
        if os.environ.get("GITHUB_DATA_BASE_URL", "").strip():
            await send_status("Fetching prebuilt session data...")
            if await asyncio.to_thread(fetch_prebuilt_session, year, round_num, session_type):
                return True

        await send_status("Session data not found — processing on demand...")

        # Use a queue to bridge sync callbacks to async WebSocket sends
        status_queue: asyncio.Queue = asyncio.Queue()

        def sync_status(msg: str):
            status_queue.put_nowait(msg)

        # Run processing in background thread
        loop = asyncio.get_event_loop()
        process_task = loop.run_in_executor(
            None,
            process_session_sync,
            year,
            round_num,
            session_type,
            False,
            sync_status,
        )

        # Forward status messages while processing
        while not process_task.done():
            try:
                msg = await asyncio.wait_for(status_queue.get(), timeout=1.0)
                await send_status(msg)
            except asyncio.TimeoutError:
                pass

        # Drain remaining messages
        while not status_queue.empty():
            msg = status_queue.get_nowait()
            await send_status(msg)

        try:
            success = process_task.result()
            return success
        except Exception as e:
            logger.error(f"On-demand processing failed for {key}: {e}")
            return False
