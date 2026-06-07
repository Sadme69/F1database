"""Compute average pit loss times per circuit using precise PitInTime/PitOutTime from FastF1.

For each circuit, calculates the median pit lane time (PitOutTime - PitInTime)
from green flag pit stops only. This gives a precise measurement of time spent
in the pit lane, unaffected by driving pace on the rest of the lap.

Usage:
    python compute_pit_loss_v2.py              # All 2025 rounds
    python compute_pit_loss_v2.py 1 2 3        # Specific rounds
    python compute_pit_loss_v2.py --year 2026  # Different year

Output: data/pit_loss.json (local) + uploaded to R2
"""

import json
import os
import sys
import logging
from pathlib import Path

import fastf1
import pandas as pd

from dotenv import load_dotenv
load_dotenv()
os.environ.setdefault("STORAGE_MODE", "r2")

from services.storage import put_json

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def analyze_round(year: int, round_num: int) -> list[dict]:
    """Analyze pit stops for a single race using FastF1 directly."""
    try:
        session = fastf1.get_session(year, round_num, "R")
        session.load(telemetry=False, weather=False)
    except Exception as e:
        logger.warning(f"  R{round_num}: Could not load session: {e}")
        return []

    laps = session.laps
    if laps is None or len(laps) == 0:
        logger.warning(f"  R{round_num}: No lap data")
        return []

    event_name = str(session.event.get("EventName", f"Round {round_num}"))
    circuit_name = str(session.event.get("Location", f"Round {round_num}"))

    records = []

    for driver in laps["Driver"].unique():
        drv_laps = laps.pick_drivers(driver).sort_values("LapNumber")

        for _, lap in drv_laps.iterrows():
            pit_in = lap.get("PitInTime")
            pit_out = lap.get("PitOutTime")

            # Need PitInTime on this lap
            if pd.isna(pit_in):
                continue

            # Skip lap 1 (formation lap)
            if int(lap["LapNumber"]) <= 1:
                continue

            # Find PitOutTime — could be on this lap or the next
            if pd.isna(pit_out):
                # Check next lap for PitOutTime
                next_lap_num = int(lap["LapNumber"]) + 1
                next_laps = drv_laps[drv_laps["LapNumber"] == next_lap_num]
                if len(next_laps) > 0:
                    pit_out = next_laps.iloc[0].get("PitOutTime")
                if pd.isna(pit_out):
                    continue

            # Calculate pit lane time
            pit_in_sec = pit_in.total_seconds()
            pit_out_sec = pit_out.total_seconds()
            pit_lane_time = pit_out_sec - pit_in_sec

            # Sanity check — pit lane time should be roughly 15-60s
            if pit_lane_time < 10 or pit_lane_time > 120:
                continue

            # Check track status — only use green flag stops
            track_status = str(lap.get("TrackStatus", "1"))
            # TrackStatus codes: 1=green, 2=yellow, 4=SC, 5=red, 6=VSC, 7=green
            is_green = track_status in ("1", "7", "1.0", "7.0")

            abbr = str(lap.get("Driver", ""))

            records.append({
                "event_name": event_name,
                "circuit": circuit_name,
                "round": round_num,
                "driver": abbr,
                "lap": int(lap["LapNumber"]),
                "pit_in_time": round(pit_in_sec, 3),
                "pit_out_time": round(pit_out_sec, 3),
                "pit_lane_time": round(pit_lane_time, 3),
                "track_status": track_status,
                "is_green": is_green,
            })

    green_count = sum(1 for r in records if r["is_green"])
    logger.info(f"  R{round_num} ({event_name}): {len(records)} pit stops ({green_count} green flag)")
    return records


def main():
    year = 2025
    rounds = None

    args = sys.argv[1:]
    filtered_args = []
    i = 0
    while i < len(args):
        if args[i] == "--year" and i + 1 < len(args):
            year = int(args[i + 1])
            i += 2
        else:
            filtered_args.append(args[i])
            i += 1

    if filtered_args:
        rounds = [int(r) for r in filtered_args]
    else:
        rounds = list(range(1, 25))

    logger.info(f"Analyzing {len(rounds)} rounds from {year} using FastF1 PitInTime/PitOutTime")

    all_records = []
    for round_num in rounds:
        logger.info(f"Processing round {round_num}...")
        records = analyze_round(year, round_num)
        all_records.extend(records)

    if not all_records:
        logger.error("No pit stop data collected!")
        return

    logger.info(f"\nTotal pit stops analyzed: {len(all_records)}")

    # Filter to green flag stops only for baseline calculation
    green_records = [r for r in all_records if r["is_green"]]
    logger.info(f"Green flag pit stops: {len(green_records)}")

    # Aggregate by circuit
    circuit_data: dict[str, list[float]] = {}
    circuit_meta: dict[str, dict] = {}

    for rec in green_records:
        circuit = rec["event_name"]
        if circuit not in circuit_data:
            circuit_data[circuit] = []
            circuit_meta[circuit] = {
                "circuit": rec["circuit"],
                "round": rec["round"],
            }
        circuit_data[circuit].append(rec["pit_lane_time"])

    def median(vals: list[float]) -> float:
        s = sorted(vals)
        return s[len(s) // 2]

    # SC/VSC factor — placeholder, will be set by user
    SC_VSC_FACTOR = 0.73  # Applied equally to SC and VSC

    pit_loss_by_circuit = {}
    for circuit, times in sorted(circuit_data.items()):
        if not times:
            continue
        meta = circuit_meta[circuit]

        # Two-pass: compute raw median, filter outliers > 1.5x median, recompute
        raw_median = median(times)
        threshold = raw_median * 1.5
        filtered = [t for t in times if t <= threshold]
        removed = len(times) - len(filtered)
        if removed > 0:
            logger.info(f"  {circuit}: removed {removed} outlier(s) > {threshold:.1f}s")
        if not filtered:
            filtered = times  # fallback if filtering removed everything

        green_median = round(median(filtered), 1)

        pit_loss_by_circuit[circuit] = {
            "event_name": circuit,
            "circuit": meta["circuit"],
            "round": meta["round"],
            "pit_loss_green": green_median,
            "pit_loss_green_count": len(filtered),
            "pit_loss_sc": round(green_median * SC_VSC_FACTOR, 1),
            "pit_loss_vsc": round(green_median * SC_VSC_FACTOR, 1),
        }

    # Global averages as fallback
    all_green_times = [r["pit_lane_time"] for r in green_records]
    global_green = round(median(all_green_times), 1) if all_green_times else 22.0

    summary = {
        "year": year,
        "method": "PitInTime/PitOutTime (precise)",
        "sc_vsc_factor": SC_VSC_FACTOR,
        "global_averages": {
            "green": global_green,
            "sc": round(global_green * SC_VSC_FACTOR, 1),
            "vsc": round(global_green * SC_VSC_FACTOR, 1),
            "green_count": len(all_green_times),
        },
        "circuits": pit_loss_by_circuit,
    }

    # Print summary table
    logger.info(f"\n{'='*70}")
    ga = summary["global_averages"]
    logger.info(f"SC/VSC factor: {SC_VSC_FACTOR:.0%} of green")
    logger.info(f"Global fallback: Green={ga['green']}s, SC={ga['sc']}s, VSC={ga['vsc']}s ({ga['green_count']} stops)")
    logger.info(f"\n{'Circuit':<30s} {'Green':>7s} {'SC/VSC':>7s} {'Stops':>6s}")
    logger.info(f"{'-'*30} {'-'*7} {'-'*7} {'-'*6}")
    for circuit, data in sorted(pit_loss_by_circuit.items()):
        logger.info(f"{circuit:<30s} {data['pit_loss_green']:>6.1f}s {data['pit_loss_sc']:>6.1f}s {data['pit_loss_green_count']:>5d}")

    # Save locally
    out_path = Path(__file__).parent / "data" / "pit_loss.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2))
    logger.info(f"\nSaved locally to {out_path}")

    # Upload to R2
    put_json("pit_loss.json", summary)
    logger.info("Uploaded to R2: pit_loss.json")

    # Save raw records locally for debugging
    raw_path = Path(__file__).parent / "data" / "pit_loss_raw.json"
    raw_path.write_text(json.dumps(all_records, indent=2))
    logger.info(f"Raw data saved to {raw_path}")


if __name__ == "__main__":
    main()
