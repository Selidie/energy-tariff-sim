"""
tariffs.py — Modular tariff definitions.

Each tariff class exposes:
  import_rate(dt) → pence/kWh at that datetime
  export_rate     → pence/kWh (flat for all types)
  standing_charge → pence/day
"""
from datetime import time, datetime
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


# ---------------------------------------------------------------------------
# Octopus flat tariff  (single static rate fetched from Octopus API)
# ---------------------------------------------------------------------------
class OctopusFlatTariff(BaseTariff):
    """
    A flat-rate Octopus tariff where the import rate is constant.
    Constructed from data already fetched by octopus_client — this class
    itself makes no network calls.
    """
    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self._import_rate    = float(cfg["import_rate"])
        self.product_code    = cfg.get("product_code", "")
        self.tariff_code     = cfg.get("tariff_code", "")
        self.gsp_region      = cfg.get("gsp_region", "")

    def import_rate(self, dt=None) -> float:
        return self._import_rate

    def to_dict(self) -> dict:
        d = super().to_dict()
        d.update({
            "type":           "octopus_flat",
            "import_rate":    self._import_rate,
            "standing_charge": self.standing_charge,
            "export_rate":    self._export_rate,
            "product_code":   self.product_code,
            "tariff_code":    self.tariff_code,
            "gsp_region":     self.gsp_region,
        })
        return d


# ---------------------------------------------------------------------------
# Octopus time-of-use tariff  (per-slot rates, e.g. Agile, Go)
# ---------------------------------------------------------------------------
class OctopusTimeOfUseTariff(BaseTariff):
    """
    A time-of-use Octopus tariff (Agile, Go, etc.) where the import rate
    varies by half-hour slot.

    `rates` is a list of dicts as returned by octopus_client.get_tariff_unit_rates():
      [{"value_inc_vat": 12.5, "valid_from": "2024-01-01T00:00:00Z",
        "valid_to": "2024-01-01T00:30:00Z"}, ...]

    import_rate(dt) does a linear scan to find the matching slot.  For large
    date ranges the list can be long (48 slots/day) so the rates are stored in
    a sorted list and looked up with bisect for O(log n) performance.
    """
    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self.product_code = cfg.get("product_code", "")
        self.tariff_code  = cfg.get("tariff_code", "")
        self.gsp_region   = cfg.get("gsp_region", "")
        self._fallback    = float(cfg.get("fallback_rate", 0.0))

        # Parse and sort rate slots
        import bisect
        self._bisect = bisect
        raw_rates    = cfg.get("rates", [])
        self._slots  = []   # list of (valid_from_ts, valid_to_ts, rate)
        for r in raw_rates:
            try:
                vf = _parse_iso(r["valid_from"])
                vt = _parse_iso(r["valid_to"]) if r.get("valid_to") else None
                self._slots.append((vf, vt, float(r["value_inc_vat"])))
            except (KeyError, ValueError):
                pass
        self._slots.sort(key=lambda x: x[0])
        self._starts = [s[0] for s in self._slots]

    def import_rate(self, dt) -> float:
        """Return the unit rate for the given tz-aware datetime."""
        if not self._slots:
            return self._fallback

        # Ensure dt is a UTC timestamp for comparison
        ts = _to_utc_timestamp(dt)

        # Find the last slot whose valid_from <= ts
        idx = self._bisect.bisect_right(self._starts, ts) - 1
        if idx < 0:
            return self._fallback

        _vf, vt, rate = self._slots[idx]
        # Check valid_to (None means open-ended)
        if vt is not None and ts >= vt:
            return self._fallback
        return rate

    def to_dict(self) -> dict:
        d = super().to_dict()
        d.update({
            "type":           "octopus_agile",
            "standing_charge": self.standing_charge,
            "export_rate":    self._export_rate,
            "product_code":   self.product_code,
            "tariff_code":    self.tariff_code,
            "gsp_region":     self.gsp_region,
            "slot_count":     len(self._slots),
        })
        return d

# ---------------------------------------------------------------------------
# Octopus day/night tariff  (two-rate, time-windowed, from Octopus API)
# ---------------------------------------------------------------------------
class OctopusDayNightTariff(BaseTariff):
    """
    A two-rate Octopus tariff (e.g. Go, Cosy, Economy 7) where the import
    rate differs between a cheap night window and a standard day rate.
    Constructed from rate slots returned by the Octopus API.
    """
    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self._day_rate    = float(cfg['day_rate'])
        self._night_rate  = float(cfg['night_rate'])
        self._night_start = _parse_time(cfg['night_start'])   # e.g. "00:30"
        self._night_end   = _parse_time(cfg['night_end'])     # e.g. "07:30"
        self.product_code = cfg.get('product_code', '')
        self.tariff_code  = cfg.get('tariff_code', '')
        self.gsp_region   = cfg.get('gsp_region', '')

    def _in_night_period(self, dt) -> bool:
        t = dt.time() if hasattr(dt, 'time') else dt
        return _in_night(t, self._night_start, self._night_end)

    def import_rate(self, dt) -> float:
        return self._night_rate if self._in_night_period(dt) else self._day_rate

    def to_dict(self) -> dict:
        d = super().to_dict()
        d.update({
            'type':            'octopus_day_night',
            'day_rate':        self._day_rate,
            'night_rate':      self._night_rate,
            'night_start':     self._night_start.strftime('%H:%M'),
            'night_end':       self._night_end.strftime('%H:%M'),
            'standing_charge': self.standing_charge,
            'export_rate':     self._export_rate,
            'product_code':    self.product_code,
            'tariff_code':     self.tariff_code,
            'gsp_region':      self.gsp_region,
        })
        return d

def _parse_time(s: str) -> time:
    h, m = s.split(':')
    return time(int(h), int(m))


def _in_night(t: time, start: time, end: time) -> bool:
    """Return True if t is within [start, end) — handles midnight wrap."""
    if start <= end:
        return start <= t < end
    # wraps midnight (e.g. 00:00 – 07:00)
    return t >= start or t < end

def _parse_iso(s: str) -> float:
    """Parse an ISO-8601 datetime string to a UTC Unix timestamp (float)."""
    s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s).timestamp()


def _to_utc_timestamp(dt) -> float:
    """Convert a pandas Timestamp or datetime to a UTC Unix timestamp."""
    from datetime import timezone as _tz
    if hasattr(dt, "timestamp"):
        return dt.timestamp()
    # pandas Timestamp
    return float(dt.value) / 1e9


# ---------------------------------------------------------------------------
# EDF tariff classes
# ---------------------------------------------------------------------------

class EdfFlatTariff(OctopusFlatTariff):
    def to_dict(self) -> dict:
        d = super().to_dict()
        d["type"] = "edf_flat"
        return d


class EdfDayNightTariff(OctopusDayNightTariff):
    def to_dict(self) -> dict:
        d = super().to_dict()
        d["type"] = "edf_day_night"
        return d


class EdfTimeOfUseTariff(OctopusTimeOfUseTariff):
    def to_dict(self) -> dict:
        d = super().to_dict()
        d["type"] = "edf_agile"
        return d


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
_TYPE_MAP = {
    'flat':             FlatTariff,
    'day_night':        DayNightTariff,
    'octopus_flat':     OctopusFlatTariff,
    'octopus_day_night': OctopusDayNightTariff,
    'octopus_agile':    OctopusTimeOfUseTariff,
    'edf_flat':          EdfFlatTariff,
    'edf_day_night':     EdfDayNightTariff,
    'edf_agile':         EdfTimeOfUseTariff,
}


def build_tariff(cfg: dict) -> BaseTariff:
    cls = _TYPE_MAP.get(cfg['type'])
    if cls is None:
        raise ValueError(f"Unknown tariff type: {cfg['type']}")
    return cls(cfg)


def load_tariffs(cfg: dict) -> list:
    return [build_tariff(t) for t in cfg.get('tariffs', [])]
