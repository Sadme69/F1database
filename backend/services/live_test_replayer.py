"""Replay .jsonStream files as if they were a live SignalR stream.

Reads files downloaded from the F1 static API (livetiming.formula1.com)
and replays them with original timing, useful for testing the live timing
feature without waiting for an actual session.

Expected directory layout:
    backend/data/live_test/{year}_{round}_{session}/
        TimingData.jsonStream
        TimingAppData.jsonStream
        ...
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import zlib
from pathlib import Path
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)

# Matches HH:MM:SS.mmm at the start of a line (with optional BOM)
_TIMESTAMP_RE = re.compile(
    r"^\ufeff?(\d{2}):(\d{2}):(\d{2})\.(\d{3})(.*)",
    re.DOTALL,
)


def _parse_timestamp(hours: str, minutes: str, seconds: str, millis: str) -> float:
    """Convert timestamp components to total seconds."""
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds) + int(millis) / 1000


class _Message:
    """A single parsed message from a .jsonStream file."""

    __slots__ = ("timestamp", "topic", "data")

    def __init__(self, timestamp: float, topic: str, data: dict) -> None:
        self.timestamp = timestamp
        self.topic = topic
        self.data = data

    def __lt__(self, other: _Message) -> bool:
        return self.timestamp < other.timestamp


class LiveTestReplayer:
    """Replays .jsonStream files with original timing.

    Parameters
    ----------
    data_dir:
        Path to the directory containing .jsonStream files.
    speed_multiplier:
        Factor by which to speed up (>1) or slow down (<1) playback.
        A value of 1.0 means real-time replay.
    """

    def __init__(self, data_dir: str, speed_multiplier: float = 1.0) -> None:
        self._data_dir = Path(data_dir)
        self._speed_multiplier = max(speed_multiplier, 0.01)  # prevent zero/negative
        self._messages: list[_Message] = []
        self._running = False
        self._current_index = 0

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    # Topics whose .json initial state is safe to load (time-invariant metadata).
    # Everything else (TimingData, LapCount, TimingStats, Position.z,
    # ExtrapolatedClock) contains end-of-session state that would corrupt the
    # replay if loaded at the start.
    _SAFE_INIT_TOPICS: set[str] = {
        "DriverList",       # names, teams, colours — static throughout session
        "TimingAppData",    # grid positions + starting compound (filtered below)
        "WeatherData",      # starting weather
        "TrackStatus",      # starting track status
        "SessionInfo",      # session metadata — static
    }

    def load(self) -> None:
        """Parse initial state (.json) and stream (.jsonStream) files into a
        sorted timeline.

        The .json files from the F1 static API contain the accumulated
        END-OF-SESSION state (final positions, all laps completed, etc.).
        We selectively load only topics whose initial state is safe/useful
        (driver metadata, starting compound) and skip topics whose end-of-
        session values would corrupt the replay (positions, gaps, lap counts).
        """
        self._messages.clear()

        if not self._data_dir.is_dir():
            raise FileNotFoundError(f"Data directory not found: {self._data_dir}")

        # ------------------------------------------------------------------
        # 1. Load safe initial state files at t = -1
        # ------------------------------------------------------------------
        init_count = 0
        for filepath in sorted(self._data_dir.glob("*.json")):
            if filepath.name.endswith(".jsonStream"):
                continue
            topic = filepath.stem  # e.g. TimingAppData.json -> TimingAppData
            is_compressed = topic.endswith(".z")
            effective_topic = topic[:-2] if is_compressed else topic

            if effective_topic not in self._SAFE_INIT_TOPICS:
                continue

            try:
                with open(filepath, "r", encoding="utf-8-sig") as f:
                    data = json.loads(f.read())
                if is_compressed and isinstance(data, str):
                    raw_bytes = base64.b64decode(data)
                    data = json.loads(zlib.decompress(raw_bytes, -zlib.MAX_WBITS))

                # Filter TimingAppData to only keep starting compound
                if effective_topic == "TimingAppData":
                    data = self._filter_timing_app_init(data)

                self._messages.append(_Message(-1.0, effective_topic, data))
                init_count += 1
                logger.debug("Loaded initial state for %s", effective_topic)
            except Exception as exc:
                logger.warning("Failed to load initial state %s: %s", filepath.name, exc)

        if init_count:
            logger.info("Loaded %d initial state files", init_count)

        # ------------------------------------------------------------------
        # 2. Load incremental stream files (.jsonStream)
        # ------------------------------------------------------------------
        stream_files = sorted(self._data_dir.glob("*.jsonStream"))
        if not stream_files and not init_count:
            logger.warning("No data files found in %s", self._data_dir)
            return

        for filepath in stream_files:
            topic = filepath.stem
            self._parse_file(filepath, topic)

        self._messages.sort()
        logger.info(
            "Loaded %d messages (%d initial + %d stream) from %s",
            len(self._messages),
            init_count,
            len(self._messages) - init_count,
            self._data_dir,
        )

    @staticmethod
    def _filter_timing_app_init(data: dict) -> dict:
        """Strip end-of-race stint data from TimingAppData initial state.

        Keep only GridPos and the first stint (index 0) with reset lap counts,
        so we get the correct starting compound without end-of-race pollution.
        """
        lines = data.get("Lines")
        if not lines or not isinstance(lines, dict):
            return data
        filtered_lines = {}
        for number, driver_data in lines.items():
            if not isinstance(driver_data, dict):
                filtered_lines[number] = driver_data
                continue
            filtered = {}
            # Keep grid position
            if "GridPos" in driver_data:
                filtered["GridPos"] = driver_data["GridPos"]
            if "RacingNumber" in driver_data:
                filtered["RacingNumber"] = driver_data["RacingNumber"]
            # Keep only first stint with reset counters
            stints = driver_data.get("Stints")
            if stints and isinstance(stints, list) and len(stints) > 0:
                s0 = dict(stints[0])
                s0["TotalLaps"] = 0
                s0["LapNumber"] = 0
                s0["StartLaps"] = 0
                s0.pop("LapTime", None)
                s0.pop("LapFlags", None)
                filtered["Stints"] = [s0]
            filtered_lines[number] = filtered
        return {"Lines": filtered_lines}

    def _parse_file(self, filepath: Path, topic: str) -> None:
        """Parse a single .jsonStream file, appending messages to the timeline."""
        # Detect .z compressed topics (e.g. Position.z.jsonStream)
        is_compressed = topic.endswith(".z")
        # Strip .z suffix for the message topic name
        effective_topic = topic[:-2] if is_compressed else topic

        count = 0
        # Read with utf-8-sig to automatically strip BOM if present
        with open(filepath, "r", encoding="utf-8-sig") as f:
            for line_num, raw_line in enumerate(f, start=1):
                line = raw_line.strip()
                if not line:
                    continue

                match = _TIMESTAMP_RE.match(line)
                if not match:
                    logger.warning(
                        "Skipping malformed line %d in %s: %s",
                        line_num,
                        filepath.name,
                        line[:80],
                    )
                    continue

                hours, minutes, seconds, millis, json_str = match.groups()
                timestamp = _parse_timestamp(hours, minutes, seconds, millis)
                json_str = json_str.strip()

                if not json_str:
                    logger.warning(
                        "Empty JSON payload at line %d in %s",
                        line_num,
                        filepath.name,
                    )
                    continue

                try:
                    data = json.loads(json_str)
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "Invalid JSON at line %d in %s: %s",
                        line_num,
                        filepath.name,
                        exc,
                    )
                    continue

                # Decompress .z topic payloads (base64 + zlib)
                if is_compressed and isinstance(data, str):
                    try:
                        raw_bytes = base64.b64decode(data)
                        decompressed = zlib.decompress(raw_bytes, -zlib.MAX_WBITS)
                        data = json.loads(decompressed)
                    except Exception:
                        logger.warning(
                            "Failed to decompress line %d in %s",
                            line_num,
                            filepath.name,
                        )
                        continue

                self._messages.append(_Message(timestamp, effective_topic, data))
                count += 1

        logger.debug("Parsed %d messages from %s", count, filepath.name)

    # ------------------------------------------------------------------
    # Replay
    # ------------------------------------------------------------------

    async def replay(
        self,
        callback: Callable[[str, dict, float], Awaitable[None]],
    ) -> None:
        """Replay messages with original timing, adjusted by speed_multiplier.

        Parameters
        ----------
        callback:
            Async function called for each message as
            ``callback(topic, data, timestamp_seconds)``.
        """
        if not self._messages:
            logger.warning("No messages loaded — nothing to replay")
            return

        self._running = True

        logger.info(
            "Starting replay from index %d (%.3fs) at %.1fx speed",
            self._current_index,
            self._messages[self._current_index].timestamp if self._current_index < len(self._messages) else 0,
            self._speed_multiplier,
        )

        prev_ts: float | None = None

        while self._running and self._current_index < len(self._messages):
            idx = self._current_index
            msg = self._messages[idx]

            # Sleep for the time delta between consecutive messages
            if prev_ts is not None:
                delta = msg.timestamp - prev_ts
                if delta > 0:
                    await asyncio.sleep(delta / self._speed_multiplier)

                # Check again after sleep in case stop() or skip was called
                if not self._running:
                    break
                # If index was moved externally (skip), reset prev_ts
                if self._current_index != idx:
                    prev_ts = None
                    continue

            try:
                await callback(msg.topic, msg.data, msg.timestamp)
            except Exception:
                logger.exception(
                    "Callback error for %s at %.3fs", msg.topic, msg.timestamp
                )

            prev_ts = msg.timestamp
            self._current_index = idx + 1

        if self._current_index >= len(self._messages):
            logger.info("Replay complete — all %d messages sent", len(self._messages))

        self._running = False

    def stop(self) -> None:
        """Stop an in-progress replay."""
        self._running = False

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def jump_to(self, timestamp_seconds: float) -> None:
        """Skip ahead to the first message at or after *timestamp_seconds*.

        If a replay is running it will continue from the new position.
        If called before ``replay()``, the next replay starts from this point.
        """
        if not self._messages:
            return

        # Binary search for the target timestamp
        lo, hi = 0, len(self._messages)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._messages[mid].timestamp < timestamp_seconds:
                lo = mid + 1
            else:
                hi = mid

        self._current_index = lo
        logger.info(
            "Jumped to index %d (%.3fs) — target was %.3fs",
            lo,
            self._messages[lo].timestamp if lo < len(self._messages) else 0,
            timestamp_seconds,
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def message_count(self) -> int:
        """Total number of loaded messages."""
        return len(self._messages)

    @property
    def duration(self) -> float:
        """Duration of the session in seconds (last timestamp minus first)."""
        if len(self._messages) < 2:
            return 0.0
        return self._messages[-1].timestamp - self._messages[0].timestamp

    @property
    def topics(self) -> list[str]:
        """Sorted list of unique topics present in the loaded data."""
        return sorted({m.topic for m in self._messages})

    @property
    def is_running(self) -> bool:
        """Whether a replay is currently in progress."""
        return self._running

    @property
    def current_timestamp(self) -> float:
        """Timestamp of the next message to be sent."""
        if not self._messages or self._current_index >= len(self._messages):
            return 0.0
        return self._messages[self._current_index].timestamp
