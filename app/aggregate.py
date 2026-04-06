"""
aggregate.py — Convert raw power readings (W) into 30-minute energy intervals (kWh).

Key decisions:
  - Resample to interval_minutes using mean power, then convert to kWh
  - Separate import and export from grid_power sign
  - Align intervals to UK billing boundaries (floor to interval)
  - Save as aggregated Parquet for fast repeated simulation
"""
import os
import logging
import pandas as pd
from pathlib import Path

log = logging.getLogger(__name__)


def aggregate(df: pd.DataFrame, interval_minutes: int = 30) -> pd.DataFrame:
    """
    Input:  raw DataFrame with timestamp index (UTC), columns include grid_power (W).
            Optional columns: pv_power, battery_power, load_power (all W).
    Output: DataFrame resampled to interval_minutes with energy columns (kWh).
    """
    if df.empty or 'grid_power' not in df.columns:
        log.error("DataFrame is empty or missing grid_power column")
        return pd.DataFrame()

    interval = f'{interval_minutes}min'
    interval_hours = interval_minutes / 60.0

    # Resample: mean power within each interval window
    resampled = df.resample(interval, label='left', closed='left').mean()

    result = pd.DataFrame(index=resampled.index)
    result.index.name = 'interval_start'

    # Grid power → import/export energy
    gp = resampled['grid_power'].fillna(0)
    result['grid_import_kwh'] = (gp.clip(lower=0) / 1000.0) * interval_hours
    result['grid_export_kwh'] = (gp.clip(upper=0).abs() / 1000.0) * interval_hours

    # Optional channels
    for col in ('pv_power', 'battery_power', 'load_power'):
        if col in resampled.columns:
            label = col.replace('_power', '_kwh')
            result[label] = (resampled[col].fillna(0).abs() / 1000.0) * interval_hours

    # Drop rows where grid data is missing entirely
    result = result.dropna(subset=['grid_import_kwh'])

    log.info("Aggregated %d intervals (%d-min) from %d raw rows",
             len(result), interval_minutes, len(df))
    return result


def save_aggregated(df: pd.DataFrame, agg_path: str, tag: str = 'latest'):
    """Save aggregated DataFrame as Parquet."""
    if df.empty:
        log.warning("Nothing to save.")
        return
    Path(agg_path).mkdir(parents=True, exist_ok=True)
    out = os.path.join(agg_path, f"agg_{tag}.parquet")
    df.to_parquet(out)
    log.info("Saved aggregated data → %s (%d intervals)", out, len(df))
    return out


def load_aggregated(agg_path: str) -> pd.DataFrame:
    """Load the most recent aggregated Parquet."""
    path = Path(agg_path)
    files = sorted(path.glob("agg_*.parquet"))
    if not files:
        log.warning("No aggregated files in %s", agg_path)
        return pd.DataFrame()
    # merge all, de-duplicate
    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames).sort_index()
    df = df[~df.index.duplicated(keep='last')]
    log.info("Loaded %d aggregated intervals from %d file(s)", len(df), len(files))
    return df


def run_aggregate(cfg: dict, raw_df: pd.DataFrame = None) -> pd.DataFrame:
    """Full aggregation pipeline: load raw → aggregate → save."""
    from app.ingest import load_raw
    interval = cfg.get('simulation', {}).get('interval_minutes', 30)
    raw_path = cfg['storage']['raw_path']
    agg_path = cfg['storage']['aggregated_path']

    if raw_df is None or raw_df.empty:
        raw_df = load_raw(raw_path)

    agg_df = aggregate(raw_df, interval)
    save_aggregated(agg_df, agg_path)
    return agg_df
