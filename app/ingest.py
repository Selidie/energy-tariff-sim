"""
ingest.py — Pull historical data from mqtt-bridge /history endpoint and
            store it as raw Parquet files, one per topic per day.
"""
import os
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def fetch_history(api_url: str, topics: list[str], range_str: str, window: str) -> dict:
    """Call mqtt-bridge /history and return the series dict."""
    url = f"{api_url.rstrip('/')}/history"
    params = {
        'topics': ','.join(topics),
        'range':  range_str,
        'window': window,
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('success'):
            log.error("History query failed: %s", data.get('error'))
            return {}
        return data.get('series', {})
    except Exception as e:
        log.error("Failed to fetch history from %s: %s", url, e)
        return {}


def series_to_dataframe(series: dict, topic_label_map: dict) -> pd.DataFrame:
    """
    Convert the /history series dict into a tidy DataFrame.
    topic_label_map: { 'total/grid_power/state': 'grid_power', ... }
    """
    frames = []
    for topic, records in series.items():
        label = topic_label_map.get(topic, topic)
        df = pd.DataFrame(records)  # columns: time, value
        df['timestamp'] = pd.to_datetime(df['time'], unit='s', utc=True)
        df = df.drop(columns=['time']).rename(columns={'value': label})
        df = df.set_index('timestamp')
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, axis=1).sort_index()
    return combined


def save_raw(df: pd.DataFrame, raw_path: str, tag: str = None):
    """Save raw DataFrame as a dated Parquet file."""
    if df.empty:
        log.warning("No data to save.")
        return
    Path(raw_path).mkdir(parents=True, exist_ok=True)
    tag = tag or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    out = os.path.join(raw_path, f"raw_{tag}.parquet")
    df.to_parquet(out)
    log.info("Saved raw data → %s (%d rows)", out, len(df))
    return out


def load_raw(raw_path: str) -> pd.DataFrame:
    """Load and concatenate all raw Parquet files in raw_path."""
    path = Path(raw_path)
    files = sorted(path.glob("raw_*.parquet"))
    if not files:
        log.warning("No raw files found in %s", raw_path)
        return pd.DataFrame()
    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames).sort_index()
    df = df[~df.index.duplicated(keep='last')]
    log.info("Loaded %d raw records from %d file(s)", len(df), len(files))
    return df


def run_ingest(cfg: dict) -> pd.DataFrame:
    """Full ingest pipeline: fetch → dataframe → save raw."""
    mqtt_cfg   = cfg['mqtt']
    api_url    = mqtt_cfg['api_url']
    topic_map  = mqtt_cfg['topics']   # label → short_topic
    sim_cfg    = cfg.get('simulation', {})
    range_str  = sim_cfg.get('history_range', '30d')
    window     = sim_cfg.get('history_window', '1m')
    raw_path   = cfg['storage']['raw_path']

    # Build reverse map: short_topic → label
    short_to_label = {v: k for k, v in topic_map.items()}
    topics = list(topic_map.values())

    log.info("Fetching history: topics=%s range=%s window=%s", topics, range_str, window)
    series = fetch_history(api_url, topics, range_str, window)

    if not series:
        log.error("No series returned — check mqtt-bridge is running and InfluxDB is enabled")
        return pd.DataFrame()

    df = series_to_dataframe(series, short_to_label)
    save_raw(df, raw_path)
    return df
