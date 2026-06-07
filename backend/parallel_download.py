"""Parallel bulk download — process many sessions at once.

Enumerates every target session (Race/Qualifying/Sprint/Sprint-Qualifying that
is 'available' in the cached schedules) and processes them across N worker
processes. Each worker gets its OWN FastF1 cache dir so the per-session
cache-delete never conflicts. Already-done sessions are skipped.

Usage (from backend/):
    DATA_DIR="../match data" python parallel_download.py --workers 6
"""

from __future__ import annotations

import argparse
import glob
import gzip
import json
import multiprocessing as mp
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

YEARS = [2024, 2025, 2026]
NAME_TO_TYPE = {"Race": "R", "Qualifying": "Q", "Sprint": "S", "Sprint Qualifying": "SQ",
                "Practice 1": "FP1", "Practice 2": "FP2", "Practice 3": "FP3"}
_CACHE_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".fastf1-cache-workers")


def enumerate_tasks(data_dir: str) -> list[tuple[int, int, str]]:
    tasks = []
    for y in YEARS:
        sf = os.path.join(data_dir, "seasons", str(y), "schedule.json")
        if not os.path.exists(sf):
            continue
        b = open(sf, "rb").read()
        if b[:2] == b"\x1f\x8b":
            b = gzip.decompress(b)
        for ev in json.loads(b).get("events", []):
            rnd = ev.get("round_number")
            for s in ev.get("sessions", []):
                if s.get("name") in NAME_TO_TYPE and s.get("available"):
                    tasks.append((y, int(rnd), NAME_TO_TYPE[s["name"]]))
    return tasks


def _init(data_dir: str) -> None:
    # Runs once per worker, before any task imports the heavy stack.
    os.environ["DATA_DIR"] = data_dir
    os.environ["STORAGE_MODE"] = "local"
    os.environ["TELEMETRY_MODE"] = "eager"
    os.environ["DELETE_FASTF1_CACHE"] = "true"
    os.environ["TELEMETRY_WORKERS"] = "2"  # low per-session: cross-session parallelism fills cores
    os.environ["FASTF1_CACHE_DIR"] = os.path.join(_CACHE_ROOT, f"w{os.getpid()}")


def _work(task: tuple[int, int, str]) -> tuple[int, int, str, bool]:
    year, rnd, stype = task
    from services.process import process_session_sync
    try:
        ok = process_session_sync(year, rnd, stype, skip_existing=True)
        return (year, rnd, stype, bool(ok))
    except Exception:
        return (year, rnd, stype, False)


def main() -> None:
    ap = argparse.ArgumentParser(description="Parallel bulk session download")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--data", default=os.environ.get("DATA_DIR"))
    args = ap.parse_args()
    if not args.data:
        sys.exit("Set DATA_DIR or pass --data")

    os.makedirs(_CACHE_ROOT, exist_ok=True)
    tasks = enumerate_tasks(args.data)
    n = len(tasks)
    print(f"Enumerated {n} target sessions; running {args.workers} workers", flush=True)

    t0 = time.time()
    done = 0
    ok_count = 0
    with mp.Pool(args.workers, initializer=_init, initargs=(args.data,)) as pool:
        for (y, r, t, ok) in pool.imap_unordered(_work, tasks):
            done += 1
            ok_count += 1 if ok else 0
            el = time.time() - t0
            rate = done / el if el else 0
            eta = (n - done) / rate if rate else 0
            print(f"[{done}/{n}] {y} R{r} {t} -> {'ok' if ok else 'skip/fail'}"
                  f"   elapsed {el/60:.1f}m  ETA {eta/60:.0f}m", flush=True)

    # tidy up the per-worker cache dirs
    shutil.rmtree(_CACHE_ROOT, ignore_errors=True)
    print(f"PARALLEL BATCH COMPLETE — {ok_count}/{n} ok in {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    mp.freeze_support()
    main()
