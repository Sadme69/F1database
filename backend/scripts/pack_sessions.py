"""Pack each processed session into its own zip, ready to push to GitHub.

Produces a tree you commit to a data repo:

    <out>/
      sessions/{year}/{round}/{type}.zip   <- one zip per session
      seasons/{year}/schedule.json         <- race-list schedules (small)
      pit_loss.json                        <- pit-loss data
      MANIFEST.json                        <- list of available sessions

The website fetches a single {type}.zip on demand, extracts it, and serves.
Files inside are already compressed (zstd/gzip), so the zip uses STORED (no
re-compression) — it's just a bundle.

Usage (from backend/):
    DATA_DIR="../match data" python scripts/pack_sessions.py --out ../github_data
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services import storage  # noqa: E402

LABEL_TO_TYPE = {v: k for k, v in storage._SESSION_TYPE_LABEL.items()}
MB = 1024 * 1024


def _round_num(folder: str) -> int | None:
    m = re.match(r"\s*0*(\d+)", folder)
    return int(m.group(1)) if m else None


def main() -> None:
    ap = argparse.ArgumentParser(description="Pack sessions into per-session zips for GitHub")
    ap.add_argument("--out", default="../github_data", help="output dir (the repo to push)")
    args = ap.parse_args()

    data = storage._data_dir()
    out = os.path.abspath(args.out)
    sessions_root = os.path.join(str(data), "sessions")
    os.makedirs(out, exist_ok=True)

    manifest = []
    total_zip = 0
    count = 0
    for year in sorted(os.listdir(sessions_root)):
        ydir = os.path.join(sessions_root, year)
        if not os.path.isdir(ydir):
            continue
        for round_folder in sorted(os.listdir(ydir)):
            rdir = os.path.join(ydir, round_folder)
            if not os.path.isdir(rdir):
                continue
            rnum = _round_num(round_folder)
            if rnum is None:
                continue
            for type_folder in sorted(os.listdir(rdir)):
                tdir = os.path.join(rdir, type_folder)
                if not os.path.isdir(tdir):
                    continue
                stype = LABEL_TO_TYPE.get(type_folder, type_folder)
                files = [os.path.join(r, f) for r, _, fs in os.walk(tdir) for f in fs]
                if not any(f.endswith("replay.meta.json") for f in files):
                    continue  # incomplete session
                zip_rel = f"sessions/{year}/{rnum}/{stype}.zip"
                zip_path = os.path.join(out, zip_rel)
                os.makedirs(os.path.dirname(zip_path), exist_ok=True)
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as z:
                    for f in files:
                        z.write(f, os.path.relpath(f, tdir).replace(os.sep, "/"))
                size = os.path.getsize(zip_path)
                total_zip += size
                count += 1
                manifest.append({
                    "year": int(year), "round": rnum, "type": stype,
                    "name": round_folder, "zip": zip_rel, "bytes": size,
                })

    # schedules + pit_loss (small metadata the site needs without FastF1)
    seasons_src = os.path.join(str(data), "seasons")
    if os.path.isdir(seasons_src):
        shutil.copytree(seasons_src, os.path.join(out, "seasons"), dirs_exist_ok=True)
    pit = os.path.join(str(data), "pit_loss.json")
    if os.path.exists(pit):
        shutil.copy2(pit, os.path.join(out, "pit_loss.json"))

    with open(os.path.join(out, "MANIFEST.json"), "w", encoding="utf-8") as fh:
        json.dump({"sessions": manifest}, fh, separators=(",", ":"))

    print(f"Packed {count} sessions -> {out}")
    print(f"Total zip size: {total_zip / MB:.1f} MB  (avg {total_zip / max(count,1) / MB:.1f} MB/session)")


if __name__ == "__main__":
    main()
