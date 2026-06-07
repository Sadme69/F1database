import asyncio
import logging

from fastapi import APIRouter, Query, HTTPException

from services.storage import exists
from services import telemetry_store

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["telemetry"])

# Per-(session, driver) locks so concurrent requests for the same driver build
# its telemetry once instead of loading the session multiple times.
_build_locks: dict[str, asyncio.Lock] = {}


def _build_driver_telemetry_sync(year: int, round_num: int, type: str, driver: str) -> dict | None:
    """Build and store one driver's full telemetry on demand (lazy mode).

    Imports the heavy FastF1 stack lazily so the serving baseline stays light.
    """
    from services.f1_data import _get_driver_telemetry_all_laps_sync

    base = f"sessions/{year}/{round_num}/{type}"
    # One channel merge per driver, sliced per lap (~12x faster than per-lap).
    drv_telemetry = _get_driver_telemetry_all_laps_sync(year, round_num, type, driver)
    if drv_telemetry:
        telemetry_store.put(base, driver, drv_telemetry)  # zstd
    return drv_telemetry or None


@router.get("/sessions/{year}/{round_num}/telemetry")
async def driver_telemetry(
    year: int,
    round_num: int,
    type: str = Query("R"),
    driver: str = Query(...),
    lap: int = Query(...),
):
    base = f"sessions/{year}/{round_num}/{type}"
    data = telemetry_store.get(base, driver)

    # Lazy build: telemetry for this driver hasn't been computed yet.
    if data is None:
        # Only attempt a build if the session itself has been processed.
        if not exists(f"{base}/replay.meta.json") and not exists(f"{base}/replay.json"):
            raise HTTPException(status_code=404, detail="Session not processed yet")

        key = f"{year}_{round_num}_{type}_{driver}"
        lock = _build_locks.setdefault(key, asyncio.Lock())
        async with lock:
            # Another request may have built it while we waited.
            data = telemetry_store.get(base, driver)
            if data is None:
                logger.info(f"Lazily building telemetry for {key}...")
                data = await asyncio.to_thread(
                    _build_driver_telemetry_sync, year, round_num, type, driver
                )

    if data is None:
        raise HTTPException(status_code=404, detail="Telemetry not available for this driver")

    lap_data = data.get(str(lap))
    if lap_data is None:
        raise HTTPException(status_code=404, detail="Telemetry not available for this lap")
    return lap_data
