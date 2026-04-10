"""
ingest.py — Pull historical data from mqtt-bridge /history endpoint and
            store it as raw Parquet files.
"""
import os
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)


def _restore_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    After pd.concat() or pd.read_parquet(), a tz-aware DatetimeIndex can
    silently degrade to a plain Index of strings/objects. Force it back.
    """
    if df.empty:
        return df
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)
    return df


def dates_to_range_str(date_from: str, date_to: str) -> str:
    """
    Convert a from/to date pair (YYYY-MM-DD strings) into a range string
    (e.g. '365d') measured back from date_to (or now if date_to is today/future).
    Returns a plain Nd string that the mqtt-bridge /history endpoint understands.
    """
    try:
        dt_from = datetime.strptime(date_from, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        dt_to   = datetime.strptime(date_to,   '%Y-%m-%d').replace(tzinfo=timezone.utc)
        # Add one day so the end date is inclusive
        dt_to   = dt_to + timedelta(days=1)
        now     = datetime.now(timezone.utc)
        # How many days from dt_from back to now is the window we need
        days = max(1, (now - dt_from).days + 1)
        return f'{days}d', dt_from, dt_to
    except ValueError as e:
        log.warning("Could not parse date range (%s → %s): %s — falling back to config range", date_from, date_to, e)
        return None, None, None


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------
def check_bridge(api_url: str) -> dict:
    url = f"{api_url.rstrip('/')}/health"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        influx = data.get('influx_enabled', False)
        mqtt   = data.get('mqtt_connected', False)
        if not mqtt:
            return {'ok': False, 'reason': 'mqtt-bridge is not connected to the MQTT broker',
                    'influx_enabled': influx, 'mqtt_connected': mqtt}
        if not influx:
            return {'ok': False, 'reason': 'InfluxDB is not enabled on mqtt-bridge — set INFLUX_URL and INFLUX_TOKEN in its .env',
                    'influx_enabled': influx, 'mqtt_connected': mqtt}
        return {'ok': True, 'reason': 'ok', 'influx_enabled': influx, 'mqtt_connected': mqtt}
    except requests.exceptions.ConnectionError:
        return {'ok': False, 'reason': f'Cannot reach mqtt-bridge at {api_url} — is it running?',
                'influx_enabled': False, 'mqtt_connected': False}
    except Exception as e:
        return {'ok': False, 'reason': f'mqtt-bridge health check failed: {e}',
                'influx_enabled': False, 'mqtt_connected': False}


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
def fetch_history(api_url: str, topics: list, range_str: str, window: str) -> dict:
    url = f"{api_url.rstrip('/')}/history"
    params = {'topics': ','.join(topics), 'range': range_str, 'window': window}
    try:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('success'):
            log.error("History query failed: %s", data.get('error', 'unknown error'))
            return {}
        series = data.get('series', {})
        if not series:
            log.warning("mqtt-bridge returned success but empty series (range=%s, topics=%s)",
                        range_str, topics)
        return series
    except Exception as e:
        log.error("Failed to fetch history from %s: %s", url, e)
        return {}


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def series_to_dataframe(series: dict, topic_label_map: dict, tz: str = 'UTC',
                        clip_from: datetime = None, clip_to: datetime = None) -> pd.DataFrame:
    """
    Convert the /history series dict into a tidy DataFrame.
    Timestamps from InfluxDB are Unix seconds, parsed as UTC then
    converted to the configured local timezone.

    If clip_from / clip_to are provided (tz-aware datetimes), the resulting
    DataFrame is sliced to that window so the user's chosen date range is
    respected even if the bridge returned a broader Nd window.
    """
    frames = []
    for topic, records in series.items():
        if not records:
            log.warning("Topic %s returned no records", topic)
            continue
        label = topic_label_map.get(topic, topic)
        df = pd.DataFrame(records)
        df['timestamp'] = pd.to_datetime(df['time'], unit='s', utc=True)
        df['timestamp'] = df['timestamp'].dt.tz_convert(tz)
        df = df.drop(columns=['time']).rename(columns={'value': label})
        df = df.set_index('timestamp')
        frames.append(df)
        log.info("Topic %-45s -> %d records  label=%s", topic, len(df), label)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, axis=1).sort_index()
    combined = _restore_datetime_index(combined)

    # Clip to the user-requested date window
    if clip_from is not None:
        cf = clip_from.astimezone(combined.index.tz) if combined.index.tz else clip_from.replace(tzinfo=None)
        combined = combined[combined.index >= cf]
    if clip_to is not None:
        ct = clip_to.astimezone(combined.index.tz) if combined.index.tz else clip_to.replace(tzinfo=None)
        combined = combined[combined.index < ct]

    log.info("Combined DataFrame: %d rows, index=%s, tz=%s",
             len(combined), type(combined.index).__name__,
             getattr(combined.index, 'tz', 'n/a'))
    return combined


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
def save_raw(df: pd.DataFrame, raw_path: str, tag: str = None) -> str | None:
    if df.empty:
        log.warning("No data to save.")
        return None
    Path(raw_path).mkdir(parents=True, exist_ok=True)
    tag = tag or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    out = os.path.join(raw_path, f"raw_{tag}.parquet")
    df.to_parquet(out)
    log.info("Saved raw data -> %s (%d rows)", out, len(df))
    return out


def load_raw(raw_path: str) -> pd.DataFrame:
    path = Path(raw_path)
    files = sorted(path.glob("raw_*.parquet"))
    if not files:
        log.warning("No raw files found in %s", raw_path)
        return pd.DataFrame()
    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames).sort_index()
    df = df[~df.index.duplicated(keep='last')]
    df = _restore_datetime_index(df)
    log.info("Loaded %d raw records from %d file(s), index=%s, tz=%s",
             len(df), len(files), type(df.index).__name__,
             getattr(df.index, 'tz', 'n/a'))
    return df


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------
def run_ingest(cfg: dict, date_from: str = None, date_to: str = None) -> tuple:
    """
    Pull history from the mqtt-bridge and store as raw Parquet.

    date_from / date_to: optional YYYY-MM-DD strings supplied by the user via
    the UI.  When provided they override the history_range in settings.yaml.
    """
    mqtt_cfg  = cfg['mqtt']
    api_url   = mqtt_cfg['api_url']
    topic_map = mqtt_cfg['topics']
    sim_cfg   = cfg.get('simulation', {})
    window    = sim_cfg.get('history_window', '1m')
    tz        = sim_cfg.get('timezone', 'UTC')
    raw_path  = cfg['storage']['raw_path']

    short_to_label = {v: k for k, v in topic_map.items()}
    topics = list(topic_map.values())

    # --- Resolve range string ---
    clip_from = clip_to = None
    if date_from and date_to:
        range_str, clip_from, clip_to = dates_to_range_str(date_from, date_to)
        if range_str is None:
            range_str = sim_cfg.get('history_range', '700d')
        log.info("User-supplied date range: %s → %s  (requesting %s from bridge)", date_from, date_to, range_str)
    else:
        range_str = sim_cfg.get('history_range', '700d')
        log.info("Using config history_range: %s", range_str)

    diag = check_bridge(api_url)
    if not diag['ok']:
        log.error("Bridge check failed: %s", diag['reason'])
        return pd.DataFrame(), diag

    log.info("Fetching history: topics=%s range=%s window=%s tz=%s", topics, range_str, window, tz)
    series = fetch_history(api_url, topics, range_str, window)

    if not series:
        diag = {'ok': False,
                'reason': (
                    f"mqtt-bridge returned no data for topics {topics} "
                    f"over range={range_str}. "
                    "Check InfluxDB has data — the bridge must have been running "
                    "long enough to record readings."
                ),
                'influx_enabled': True, 'mqtt_connected': True}
        return pd.DataFrame(), diag

    df = series_to_dataframe(series, short_to_label, tz=tz,
                             clip_from=clip_from, clip_to=clip_to)
    save_raw(df, raw_path)

    diag['rows'] = len(df)
    diag['topics_found'] = list(series.keys())
    diag['date_from'] = str(df.index.min())
    diag['date_to']   = str(df.index.max())
    return df, diag
