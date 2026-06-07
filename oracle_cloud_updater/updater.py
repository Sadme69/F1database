#!/usr/bin/env python3
"""Hourly F1 data updater — designed to run on an Oracle Cloud Free Tier VM.

Each run:
  1. Pulls every season's schedule from FastF1 and writes it to the store(s).
  2. Compares FastF1's available sessions against what's already in the store.
  3. For anything NEW (or recently updated — to catch late data corrections),
     downloads it via FastF1, bakes full telemetry, packs it into a per-session
     zip, and publishes it.
  4. Refreshes MANIFEST.json and commits/uploads.

Destinations are pluggable (see publish.py): DEST=github | oracle | github,oracle.

This reuses the app's own backend services (services.f1_data / services.process /
services.storage), so it must run from a checkout of the app repo with the
backend deps installed. setup.sh wires that up on the VM.

Config (env / .env — see .env.example):
  DEST            github | oracle | github,oracle      (default github)
  SEASONS         comma list of years                  (default 2024,2025,2026)
  REFRESH_DAYS    re-process sessions newer than N days (default 2)
  MAX_PER_RUN     cap sessions processed per run, 0=∞   (default 0)
  TELEMETRY_WORKERS  threads per session                (default 4 — good on A1)
  + the GITHUB_* / ORACLE_* vars the chosen publisher needs.
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import sys
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
BACKEND = os.path.join(REPO_ROOT, "backend")

# Load .env (next to this file) before anything reads os.environ.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(HERE, ".env"))
except Exception:
    pass

# --- backend runtime config (must be set before importing services) -------- #
WORK = os.path.join(HERE, "_work")
os.environ["STORAGE_MODE"] = "local"
os.environ["DATA_DIR"] = WORK
os.environ["TELEMETRY_MODE"] = "eager"   # bake full telemetry into the zips
os.environ["DELETE_FASTF1_CACHE"] = "true"
os.environ.setdefault("FASTF1_CACHE_DIR", os.path.join(HERE, "_fastf1cache"))
os.environ.setdefault("TELEMETRY_WORKERS", os.environ.get("TELEMETRY_WORKERS", "4"))

sys.path.insert(0, BACKEND)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("updater")

from publish import build_publishers, pack_session_zip  # noqa: E402

TYPES = {"R", "Q", "S", "SQ", "FP1", "FP2", "FP3"}
SEASONS = [int(y) for y in os.environ.get("SEASONS", "2024,2025,2026").split(",") if y.strip()]
REFRESH_DAYS = float(os.environ.get("REFRESH_DAYS", "2"))
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "0"))


def _round_name(rnd: int, event: dict) -> str:
    """MANIFEST 'name' field, e.g. '01 - Bahrain Grand Prix'."""
    return f"{rnd:02d} - {event.get('event_name') or event.get('country') or 'Round'}"


def _is_recent(date_str: str | None, now: datetime) -> bool:
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        return (now - dt) <= timedelta(days=REFRESH_DAYS)
    except Exception:
        return False


def main() -> int:
    from services.f1_data import _get_season_events_sync, SESSION_NAME_TO_TYPE
    from services.process import process_session_sync
    from services import storage

    now = datetime.now(timezone.utc)
    pubs = build_publishers()
    for p in pubs:
        p.prepare()

    # 1) Refresh schedules first (cheap) so the season picker stays current even
    #    if session processing is capped or partially fails.
    schedules: dict[int, list] = {}
    for year in SEASONS:
        try:
            events = _get_season_events_sync(year)
            schedules[year] = events
            gz = gzip.compress(
                json.dumps({"year": year, "events": events}, separators=(",", ":")).encode(), 6)
            for p in pubs:
                p.put_schedule(year, gz)
        except Exception as exc:
            logger.warning("[schedule] %s failed: %s", year, exc)

    # 2) Find work: every available targeted session that some publisher is
    #    missing, plus recent ones (re-process to catch late corrections).
    processed, failed = [], []
    cap_hit = False
    for year, events in schedules.items():
        if cap_hit:
            break
        for ev in events:
            if cap_hit:
                break
            rnd = ev.get("round_number")
            for s in ev.get("sessions", []):
                stype = SESSION_NAME_TO_TYPE.get(s.get("name"))
                if stype not in TYPES or not s.get("available"):
                    continue
                recent = _is_recent(s.get("date_utc"), now)
                # which publishers need this session?
                need = [p for p in pubs if recent or not p.has(year, rnd, stype)]
                if not need:
                    continue

                tag = f"{year} R{rnd} {stype}"
                logger.info("[process] %s (%s)", tag, "refresh" if recent else "new")
                try:
                    ok = process_session_sync(year, rnd, stype, skip_existing=False)
                    if not ok:
                        failed.append(tag)
                        continue
                    sdir = os.path.dirname(str(
                        storage.local_path(f"sessions/{year}/{rnd}/{stype}/replay.meta.json")))
                    if not os.path.exists(os.path.join(sdir, "replay.meta.json")):
                        failed.append(tag)
                        continue
                    zip_bytes = pack_session_zip(sdir)
                    name = _round_name(rnd, ev)
                    for p in need:
                        p.put_session(year, rnd, stype, name, zip_bytes)
                    processed.append(tag)
                    logger.info("[ok] %s (%.1f MB) -> %s",
                                tag, len(zip_bytes) / 1048576, ", ".join(p.name for p in need))
                except Exception as exc:
                    logger.exception("[fail] %s: %s", tag, exc)
                    failed.append(tag)
                finally:
                    try:
                        shutil.rmtree(sdir, ignore_errors=True)  # type: ignore[name-defined]
                    except Exception:
                        pass

                if MAX_PER_RUN and len(processed) >= MAX_PER_RUN:
                    logger.info("[cap] hit MAX_PER_RUN=%d; finalizing, next run resumes", MAX_PER_RUN)
                    cap_hit = True
                    break

    # 3) Flush MANIFEST + commit/upload — even if nothing processed, schedules
    #    may have changed.
    summary = f"{len(processed)} processed, {len(failed)} failed"
    for p in pubs:
        try:
            p.finalize(summary)
        except Exception:
            logger.exception("[finalize] %s failed", p.name)

    shutil.rmtree(WORK, ignore_errors=True)
    logger.info("=== %s ===", summary)
    if processed:
        logger.info("processed: %s", ", ".join(processed))
    if failed:
        logger.info("failed: %s", ", ".join(failed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
