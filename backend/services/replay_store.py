"""Memory-light + space-light replay frame storage (V3+).

Replay frames are stored as INDIVIDUALLY-COMPRESSED records so the server can
still seek to and decode exactly one frame at a time (flat memory), while the
on-disk footprint shrinks ~18x vs raw JSON:

  - ``replay.zst``       — each frame: float-rounded compact JSON, zstd-compressed
                           with a shared per-session dictionary, concatenated.
  - ``replay.dict``      — the trained zstd dictionary (~100KB, shared by all frames).
  - ``replay.meta.json`` — small sidecar: per-frame byte offsets + timestamp/lap,
                           totals, qualifying phase markers, codec info.

For one F1 race this turns ~168MB of JSONL into ~9-10MB. The dictionary captures
the heavily-repeated keys/strings (driver names, team colours, field names);
rounding floats to display precision removes entropy compression can't touch.
"""

from __future__ import annotations

import json
import logging
import math
import os

import zstandard as zstd

from services import storage

logger = logging.getLogger(__name__)

META_VERSION = 2
# ~18x ratio in seconds; level 19 saves <10% for ~30x the time. Override via env.
ZSTD_LEVEL = int(os.environ.get("REPLAY_ZSTD_LEVEL", "12"))
DICT_SIZE = 112 * 1024   # trained dictionary target size


def _round_frame(frame: dict) -> dict:
    """Trim float precision to what the UI actually uses (big compression win)."""
    if isinstance(frame.get("timestamp"), float):
        frame["timestamp"] = round(frame["timestamp"], 2)
    for drv in frame.get("drivers", []):
        for key, val in list(drv.items()):
            if isinstance(val, float):
                if math.isnan(val) or math.isinf(val):
                    drv[key] = None            # also sanitize NaN/Inf here
                elif key in ("x", "y"):
                    drv[key] = round(val, 5)    # normalized 0-1 → sub-metre on track
                elif key == "relative_distance":
                    drv[key] = round(val, 6)
                elif key == "rpm":
                    drv[key] = round(val)
                elif key in ("speed", "throttle"):
                    drv[key] = round(val, 1)
    return frame


def store_frames(base: str, frames: list[dict]) -> dict:
    """Write ``{base}/replay.zst`` + ``replay.dict`` + ``replay.meta.json``.

    ``base`` is e.g. ``sessions/2024/1/R``. Returns the meta dict.
    """
    samples: list[bytes] = []
    timestamps: list[float] = []
    laps: list[int] = []
    quali_phases: list[dict] = []
    seen_phases: set = set()

    for f in frames:
        _round_frame(f)
        samples.append(json.dumps(f, separators=(",", ":")).encode())
        timestamps.append(f.get("timestamp", 0))
        laps.append(f.get("lap", 0))
        qp = f.get("quali_phase")
        if qp and qp.get("phase") not in seen_phases:
            seen_phases.add(qp["phase"])
            quali_phases.append({"phase": qp["phase"], "timestamp": f.get("timestamp", 0)})

    # Train a shared dictionary over the frames (skip for tiny sessions where
    # training is ineffective or can fail).
    zdict = None
    if len(samples) >= 8:
        try:
            zdict = zstd.train_dictionary(DICT_SIZE, samples)
        except Exception as e:
            logger.warning(f"zstd dictionary training failed for {base}: {e}; compressing without dict")
            zdict = None

    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL, dict_data=zdict) if zdict \
        else zstd.ZstdCompressor(level=ZSTD_LEVEL)

    offsets: list[int] = []
    buf = bytearray()
    off = 0
    for s in samples:
        blob = cctx.compress(s)
        offsets.append(off)
        buf += blob
        off += len(blob)

    storage.put_bytes(f"{base}/replay.zst", bytes(buf))
    if zdict is not None:
        storage.put_bytes(f"{base}/replay.dict", zdict.as_bytes())

    meta = {
        "version": META_VERSION,
        "codec": "zstd",
        "has_dict": zdict is not None,
        "total_frames": len(frames),
        "total_time": frames[-1].get("timestamp", 0) if frames else 0,
        "total_laps": frames[-1].get("total_laps", 0) if frames else 0,
        "quali_phases": quali_phases if quali_phases else None,
        "file_size": off,
        "offsets": offsets,
        "timestamps": timestamps,
        "laps": laps,
    }
    storage.put_bytes(
        f"{base}/replay.meta.json",
        json.dumps(meta, separators=(",", ":")).encode(),
    )
    logger.info(f"Stored {len(frames)} frames for {base} ({off / 1048576:.1f}MB compressed)")
    return meta


class FrameReader:
    """Random-access, memory-light view over a session's replay frames.

    Holds only the small index arrays + the ~100KB dictionary in memory; frame
    bodies are read and zstd-decoded one at a time, never all retained at once.
    """

    def __init__(self, base: str, meta: dict, dict_bytes: bytes | None):
        self.base = base
        self.meta = meta
        self.data_path = f"{base}/replay.zst"
        self.n: int = meta["total_frames"]
        self.file_size: int = meta["file_size"]
        self.offsets: list[int] = meta["offsets"]
        self.timestamps: list[float] = meta["timestamps"]
        self.laps: list[int] = meta["laps"]
        self.total_time = meta.get("total_time", 0)
        self.total_laps = meta.get("total_laps", 0)
        self.quali_phases = meta.get("quali_phases")
        zdict = zstd.ZstdCompressionDict(dict_bytes) if dict_bytes else None
        self._dctx = zstd.ZstdDecompressor(dict_data=zdict) if zdict else zstd.ZstdDecompressor()
        self._fh = None  # cached local file handle
        self._local = storage.local_path(self.data_path) if storage.is_local() else None

    def __len__(self) -> int:
        return self.n

    def __bool__(self) -> bool:
        return self.n > 0

    def _read_blob(self, i: int) -> bytes:
        start = self.offsets[i]
        end = self.offsets[i + 1] if i + 1 < self.n else self.file_size
        length = end - start
        if self._local is not None:
            if self._fh is None:
                self._fh = open(self._local, "rb")
            self._fh.seek(start)
            return self._fh.read(length)
        return storage.get_range(self.data_path, start, length)

    def __getitem__(self, i: int) -> dict:
        if i < 0:
            i += self.n
        if i < 0 or i >= self.n:
            raise IndexError(i)
        return json.loads(self._dctx.decompress(self._read_blob(i)))

    def __iter__(self):
        # Sequential pass — read the whole compressed file once, split by offsets,
        # decode one frame at a time (one frame in RAM).
        if self._local is not None:
            with open(self._local, "rb") as fh:
                data = fh.read()
        else:
            data = storage.get_bytes(self.data_path) or b""
        for i in range(self.n):
            start = self.offsets[i]
            end = self.offsets[i + 1] if i + 1 < self.n else self.file_size
            yield json.loads(self._dctx.decompress(data[start:end]))

    def index_for_time(self, target_time: float) -> int:
        """First frame index whose timestamp >= target_time."""
        import bisect
        i = bisect.bisect_left(self.timestamps, target_time)
        return min(i, self.n - 1) if self.n else 0

    def index_for_lap(self, target_lap: int) -> int:
        """First frame index whose lap >= target_lap."""
        import bisect
        i = bisect.bisect_left(self.laps, target_lap)
        return min(i, self.n - 1) if self.n else 0

    def close(self):
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception:
                pass
            self._fh = None


def open_reader(base: str) -> FrameReader | None:
    """Return a FrameReader for ``{base}`` if its meta sidecar exists, else None."""
    raw = storage.get_bytes(f"{base}/replay.meta.json")
    if raw is None:
        return None
    meta = json.loads(raw)
    dict_bytes = storage.get_bytes(f"{base}/replay.dict") if meta.get("has_dict") else None
    return FrameReader(base, meta, dict_bytes)
