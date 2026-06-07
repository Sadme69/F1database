#!/usr/bin/env python3
"""Generate a human-readable name index of stored match data.

Walks every processed session under DATA_DIR, reads its (gzipped) info.json, and
writes an INDEX.md at the DATA_DIR root so races are findable by name even though
the folders are keyed numerically as sessions/{year}/{round}/{type}.

Usage (from backend/):
    DATA_DIR="../match data" python scripts/index_match_data.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services import storage  # noqa: E402

SESSION_TYPE = {
    "R": "Race", "Q": "Qualifying", "S": "Sprint", "SQ": "Sprint Qualifying",
    "FP1": "Practice 1", "FP2": "Practice 2", "FP3": "Practice 3",
}


def _folder_mb(path: str) -> float:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total / 1048576


def main() -> None:
    data_dir = storage.local_path("")  # DATA_DIR root
    sessions_root = os.path.join(str(data_dir), "sessions")
    rows = []
    if os.path.isdir(sessions_root):
        for year in sorted(os.listdir(sessions_root)):
            for rnd in sorted(os.listdir(os.path.join(sessions_root, year))):
                rdir = os.path.join(sessions_root, year, rnd)
                if not os.path.isdir(rdir):
                    continue
                for stype in sorted(os.listdir(rdir)):
                    base = f"sessions/{year}/{rnd}/{stype}"
                    info = storage.get_json(f"{base}/info.json")
                    if not info:
                        continue
                    rows.append({
                        "year": info.get("year", year),
                        "round": info.get("round_number", rnd),
                        "stype": stype,
                        "event": info.get("event_name", "?"),
                        "circuit": info.get("circuit", ""),
                        "country": info.get("country", ""),
                        "folder": base,
                        "mb": _folder_mb(os.path.join(rdir, stype)),
                    })

    rows.sort(key=lambda r: (r["year"], int(str(r["round"]).lstrip("0") or 0), r["stype"]))

    lines = [
        "# Match Data Index", "",
        "Compressed F1 session data. Find a race by name below; **Folder** is its "
        "path under this data directory.", "",
        "| Event | Session | Year | Round | Circuit | Folder | Size |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['event']} | {SESSION_TYPE.get(r['stype'], r['stype'])} | "
            f"{r['year']} | {r['round']} | {r['circuit']}, {r['country']} | "
            f"`{r['folder']}` | {r['mb']:.1f} MB |"
        )
    total = sum(r["mb"] for r in rows)
    lines += ["", f"**{len(rows)} sessions, {total:.1f} MB total.**"]

    out = os.path.join(str(data_dir), "INDEX.md")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"Wrote {out} ({len(rows)} sessions, {total:.1f} MB)")


if __name__ == "__main__":
    main()
