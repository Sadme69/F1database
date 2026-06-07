from __future__ import annotations

from pydantic import BaseModel


class SeasonEvent(BaseModel):
    round_number: int
    country: str
    event_name: str
    location: str
    event_date: str
    sessions: list[str]


class DriverInfo(BaseModel):
    abbreviation: str
    driver_number: str
    full_name: str
    team_name: str
    team_color: str


class SessionInfo(BaseModel):
    year: int
    round_number: int
    event_name: str
    circuit: str
    country: str
    session_type: str
    drivers: list[DriverInfo]


class TrackPoint(BaseModel):
    x: float
    y: float


class TrackData(BaseModel):
    track_points: list[TrackPoint]
    rotation: float
    circuit_name: str


class LapEntry(BaseModel):
    driver: str
    lap_number: int
    position: int | None
    lap_time: str | None
    sector1: str | None
    sector2: str | None
    sector3: str | None
    compound: str | None
    tyre_life: int | None
    pit_in: bool
    pit_out: bool


class ReplayFrame(BaseModel):
    timestamp: float
    lap: int
    total_laps: int
    drivers: list[dict]
    status: str  # green, yellow, red, sc, vsc


class RaceResult(BaseModel):
    position: int | None
    driver: str
    abbreviation: str
    team: str
    team_color: str
    grid_position: int | None
    status: str
    points: float
    fastest_lap: str | None
    gap_to_leader: str | None
