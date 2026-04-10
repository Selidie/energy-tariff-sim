"""
tariffs.py — Modular tariff definitions.

Each tariff class exposes:
  import_rate(dt) → pence/kWh at that datetime
  export_rate     → pence/kWh (flat for all types)
  standing_charge → pence/day
"""
from datetime import time
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------
class BaseTariff:
    def __init__(self, cfg: dict):
        self.id              = cfg['id']
        self.name            = cfg['name']
        self.standing_charge = float(cfg.get('standing_charge', 0))   # p/day
        self._export_rate    = float(cfg.get('export_rate', 0))       # p/kWh

    def import_rate(self, dt) -> float:
        """Return import rate (p/kWh) for the given tz-aware datetime."""
        raise NotImplementedError

    def export_rate(self, dt=None) -> float:
        return self._export_rate

    def to_dict(self) -> dict:
        return {'id': self.id, 'name': self.name, 'type': self.__class__.__name__}


# ---------------------------------------------------------------------------
# Flat rate
# ---------------------------------------------------------------------------
class FlatTariff(BaseTariff):
    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self._import_rate = float(cfg['import_rate'])

    def import_rate(self, dt=None) -> float:
        return self._import_rate


# ---------------------------------------------------------------------------
# Day / Night (Economy 7 style)
# ---------------------------------------------------------------------------
class DayNightTariff(BaseTariff):
    def __init__(self, cfg: dict):
        super().__init__(cfg)
        day   = cfg['day']
        night = cfg['night']
        self._day_rate    = float(day['rate'])
        self._night_rate  = float(night['rate'])
        self._night_start = _parse_time(night['start'])
        self._night_end   = _parse_time(night['end'])

    def _in_night_period(self, dt) -> bool:
        """Return True if the given datetime falls in the night-rate window."""
        if hasattr(dt, 'time'):
            t = dt.time()
        elif isinstance(dt, time):
            t = dt
        else:
            t = dt
        return _in_night(t, self._night_start, self._night_end)

    def import_rate(self, dt) -> float:
        # The index is already in the configured local timezone from ingest,
        # so just extract the wall-clock time directly — no conversion needed.
        if self._in_night_period(dt):
            return self._night_rate
        return self._day_rate


def _parse_time(s: str) -> time:
    h, m = s.split(':')
    return time(int(h), int(m))


def _in_night(t: time, start: time, end: time) -> bool:
    """Return True if t is within [start, end) — handles midnight wrap."""
    if start <= end:
        return start <= t < end
    # wraps midnight (e.g. 00:00 – 07:00)
    return t >= start or t < end


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
_TYPE_MAP = {
    'flat':      FlatTariff,
    'day_night': DayNightTariff,
}


def build_tariff(cfg: dict) -> BaseTariff:
    cls = _TYPE_MAP.get(cfg['type'])
    if cls is None:
        raise ValueError(f"Unknown tariff type: {cfg['type']}")
    return cls(cfg)


def load_tariffs(cfg: dict) -> list:
    return [build_tariff(t) for t in cfg.get('tariffs', [])]
