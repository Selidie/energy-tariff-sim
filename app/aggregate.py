"""
aggregate.py — Convert raw grid_power readings (W) into 30-minute energy intervals (kWh).

Only grid_power is required:
  - positive values = import from grid
  - negative values = export to grid
"""
import os
import logging
import pandas as pd
from pathlib import Path

log = logging.getLogger(__name__)


def _restore_datetime_index(df: pd.DataFrame, tz: str = None) -> pd.DataFrame:
    """
    After pd.concat() or pd.read_parquet(), a tz-aware DatetimeIndex can
    silently degrade to a plain Index of strings/objects. Force it back.
    If tz is supplied and the restored index is tz-naive, localise to that tz.
    """
    if df.empty:
        return df
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)
        if tz and str(df.index.tz) != tz:
            df.index = df.index.tz_convert(tz)
    elif df.index.tz is None and tz:
        df.index = df.index.tz_localize(tz)
    return df


def aggregate(df: pd.DataFrame, interval_minutes: int = 30, tz: str = 'UTC') -> pd.DataFrame:
    if df.empty or 'grid_power' not in df.columns:
        log.error("DataFrame is empty or missing grid_power column")
        return pd.DataFrame()

    df = _restore_datetime_index(df, tz)
    log.info("Aggregating %d raw rows, index=%s, tz=%s",
             len(df), type(df.index).__name__, getattr(df.index, 'tz', 'n/a'))

    interval       = f'{interval_minutes}min'
    interval_hours = interval_minutes / 60.0

    resampled = df[['grid_power']].resample(interval, label='left', closed='left').mean()

    result = pd.DataFrame(index=resampled.index)
    result.index.name = 'interval_start'

    gp = resampled['grid_power'].fillna(0)
    result['grid_import_kwh'] = (gp.clip(lower=0) / 1000.0) * interval_hours
    result['grid_export_kwh'] = (gp.clip(upper=0).abs() / 1000.0) * interval_hours

    result = result.dropna(subset=['grid_import_kwh'])

    log.info("Aggregated %d intervals (%d-min), tz=%s",
             len(result), interval_minutes, getattr(result.index, 'tz', 'n/a'))
    return result


def save_aggregated(df: pd.DataFrame, agg_path: str, tag: str = 'latest'):
    if df.empty:
        log.warning("Nothing to save.")
        return
    Path(agg_path).mkdir(parents=True, exist_ok=True)
    out = os.path.join(agg_path, f"agg_{tag}.parquet")
    df.to_parquet(out)
    log.info("Saved aggregated data -> %s (%d intervals)", out, len(df))
    return out


def load_aggregated(agg_path: str) -> pd.DataFrame:
    path = Path(agg_path)
    files = sorted(path.glob("agg_*.parquet"))
    if not files:
        log.warning("No aggregated files in %s", agg_path)
        return pd.DataFrame()
    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames).sort_index()
    df = df[~df.index.duplicated(keep='last')]
    df = _restore_datetime_index(df)
    log.info("Loaded %d aggregated intervals from %d file(s), index=%s, tz=%s",
             len(df), len(files), type(df.index).__name__,
             getattr(df.index, 'tz', 'n/a'))
    return df


def run_aggregate(cfg: dict, raw_df: pd.DataFrame = None) -> pd.DataFrame:
    from app.ingest import load_raw
    sim_cfg  = cfg.get('simulation', {})
    interval = sim_cfg.get('interval_minutes', 30)
    tz       = sim_cfg.get('timezone', 'UTC')
    raw_path = cfg['storage']['raw_path']
    agg_path = cfg['storage']['aggregated_path']

    if raw_df is None or raw_df.empty:
        raw_df = load_raw(raw_path)

    agg_df = aggregate(raw_df, interval, tz=tz)
    save_aggregated(agg_df, agg_path)
    return agg_df
