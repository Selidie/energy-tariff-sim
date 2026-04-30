"""
ingest.py — Pull historical data from InfluxDB directly (primary path) or
            fall back to the mqtt-bridge /history endpoint.

Production path: Direct InfluxDB query, chunked into 7-day windows per topic
to avoid loading 95M points in a single Flux query and to bypass the 60-second
HTTP timeout that was killing the mqtt-bridge route.

Dev/fallback path: mqtt-bridge /history endpoint (unchanged behaviour).
Triggered automatically when INFLUX_URL / INFLUX_TOKEN are not set in the env.

InfluxDB schema (written by mqtt-bridge):
  measurement : "solar"                       (fixed)
  tag topic   : "inverter_1/grid_power/state" (short topic, prefix stripped)
  field       : "value"                       (fixed)
"""
import os
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

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


def dates_to_range_str(date_from: str, date_to: str) -> tuple:
    """
    Convert a from/to date pair (YYYY-MM-DD strings) into a range string
    (e.g. '365d') measured back from date_to (or now if date_to is today/future).
    Returns (range_str, dt_from, dt_to) or (None, None, None) on error.
    """
    try:
        dt_from = datetime.strptime(date_from, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        dt_to   = datetime.strptime(date_to,   '%Y-%m-%d').replace(tzinfo=timezone.utc)
        # Add one day so the end date is inclusive
        dt_to = dt_to + timedelta(days=1)
        now   = datetime.now(timezone.utc)
        days  = max(1, (now - dt_from).days + 1)
        return f'{days}d', dt_from, dt_to
    except ValueError as e:
        log.warning(
            "Could not parse date range (%s → %s): %s — falling back to config range",
            date_from, date_to, e
        )
        return None, None, None


# ---------------------------------------------------------------------------
# InfluxDB direct path
# ---------------------------------------------------------------------------

def _influx_env() -> dict | None:
    """
    Read InfluxDB connection details from environment variables injected by
    the Portainer stack (INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET).
    Returns None if any required variable is missing.
    """
    url    = os.environ.get('INFLUX_URL')
    token  = os.environ.get('INFLUX_TOKEN')
    org    = os.environ.get('INFLUX_ORG', 'home')
    bucket = os.environ.get('INFLUX_BUCKET', 'solar')
    if not url or not token:
        return None
    return {'url': url, 'token': token, 'org': org, 'bucket': bucket}


def _build_flux_chunk(bucket: str, topic: str,
                      start: datetime, stop: datetime, window: str) -> str:
    """
    Build a Flux query for a single topic over a bounded time chunk.

    mqtt-bridge writes data with this schema:
      measurement = "solar"       (fixed literal)
      tag: topic  = <short_topic> (e.g. "inverter_1/grid_power/state")
      field       = "value"       (fixed literal)

    We filter on measurement + field + topic tag, then aggregateWindow.
    """
    start_rfc = start.strftime('%Y-%m-%dT%H:%M:%SZ')
    stop_rfc  = stop.strftime('%Y-%m-%dT%H:%M:%SZ')
    return f"""
from(bucket: "{bucket}")
  |> range(start: {start_rfc}, stop: {stop_rfc})
  |> filter(fn: (r) => r._measurement == "solar")
  |> filter(fn: (r) => r._field == "value")
  |> filter(fn: (r) => r.topic == "{topic}")
  |> aggregateWindow(every: {window}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value"])
""".strip()


def fetch_history_direct(
    influx_env: dict,
    topics: list[str],
    dt_from: datetime,
    dt_to: datetime,
    window: str = '1m',
    chunk_days: int = 7,
) -> dict:
    """
    Query InfluxDB directly, chunked into `chunk_days`-day windows per topic.

    Returns a series dict identical to what mqtt-bridge /history returns:
      { topic: [ {'time': <unix_seconds>, 'value': <float>}, ... ], ... }

    Chunking keeps individual Flux queries small (7 days x 1-min aggregation
    ~10k points per topic) and avoids the memory / timeout issues that come
    from querying 469 days in one shot across NAS-backed InfluxDB.
    """
    try:
        from influxdb_client import InfluxDBClient
    except ImportError:
        log.error("influxdb-client not installed — cannot use direct InfluxDB path")
        return {}

    url    = influx_env['url']
    token  = influx_env['token']
    org    = influx_env['org']
    bucket = influx_env['bucket']

    series = {topic: [] for topic in topics}
    total_chunks = 0
    total_points = 0

    log.info(
        "Direct InfluxDB query: url=%s org=%s bucket=%s topics=%s "
        "range=%s->%s window=%s chunk=%dd",
        url, org, bucket, topics,
        dt_from.strftime('%Y-%m-%d'), dt_to.strftime('%Y-%m-%d'),
        window, chunk_days,
    )

    # timeout=300_000 ms (5 min) per chunk — generous but per-chunk so it's safe
    with InfluxDBClient(url=url, token=token, org=org, timeout=300_000) as client:
        query_api = client.query_api()

        for topic in topics:
            log.info("  topic=%s", topic)

            chunk_start  = dt_from
            topic_points = 0

            while chunk_start < dt_to:
                chunk_end = min(chunk_start + timedelta(days=chunk_days), dt_to)
                flux = _build_flux_chunk(bucket, topic, chunk_start, chunk_end, window)

                try:
                    # Each chunk is 7 days of 1-min aggregates ~10k points —
                    # safe to load with query() without memory pressure.
                    chunk_records = []
                    tables = query_api.query(flux, org=org)
                    for table in tables:
                        for record in table.records:
                            t = record.get_time()
                            v = record.get_value()
                            if t is not None and v is not None:
                                chunk_records.append({
                                    'time':  int(t.timestamp()),
                                    'value': float(v),
                                })

                    series[topic].extend(chunk_records)
                    topic_points += len(chunk_records)
                    total_chunks += 1

                    log.debug(
                        "    chunk %s->%s: %d points",
                        chunk_start.strftime('%Y-%m-%d'),
                        chunk_end.strftime('%Y-%m-%d'),
                        len(chunk_records),
                    )

                except Exception as e:
                    log.error(
                        "    chunk %s->%s FAILED for topic %s: %s",
                        chunk_start.strftime('%Y-%m-%d'),
                        chunk_end.strftime('%Y-%m-%d'),
                        topic, e,
                    )

                chunk_start = chunk_end

            log.info("  topic=%s total points=%d", topic, topic_points)
            total_points += topic_points

    log.info(
        "Direct InfluxDB fetch complete: %d topics, %d chunks, %d total points",
        len(topics), total_chunks, total_points,
    )
    return series


# ---------------------------------------------------------------------------
# mqtt-bridge fallback path
# ---------------------------------------------------------------------------

def check_bridge(api_url: str) -> dict:
    url = f"{api_url.rstrip('/')}/health"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data   = resp.json()
        influx = data.get('influx_enabled', False)
        mqtt   = data.get('mqtt_connected', False)
        if not mqtt:
            return {
                'ok': False,
                'reason': 'mqtt-bridge is not connected to the MQTT broker',
                'influx_enabled': influx,
                'mqtt_connected': mqtt,
            }
        if not influx:
            return {
                'ok': False,
                'reason': (
                    'InfluxDB is not enabled on mqtt-bridge — '
                    'set INFLUX_URL and INFLUX_TOKEN in its .env'
                ),
                'influx_enabled': influx,
                'mqtt_connected': mqtt,
            }
        return {'ok': True, 'reason': 'ok', 'influx_enabled': influx, 'mqtt_connected': mqtt}
    except requests.exceptions.ConnectionError:
        return {
            'ok': False,
            'reason': f'Cannot reach mqtt-bridge at {api_url} — is it running?',
            'influx_enabled': False,
            'mqtt_connected': False,
        }
    except Exception as e:
        return {
            'ok': False,
            'reason': f'mqtt-bridge health check failed: {e}',
            'influx_enabled': False,
            'mqtt_connected': False,
        }


def fetch_history(api_url: str, topics: list, range_str: str, window: str) -> dict:
    url    = f"{api_url.rstrip('/')}/history"
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
            log.warning(
                "mqtt-bridge returned success but empty series (range=%s, topics=%s)",
                range_str, topics,
            )
        return series
    except Exception as e:
        log.error("Failed to fetch history from %s: %s", url, e)
        return {}


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def series_to_dataframe(
    series: dict,
    topic_label_map: dict,
    tz: str = 'UTC',
    clip_from: datetime = None,
    clip_to: datetime = None,
) -> pd.DataFrame:
    """
    Convert the series dict into a tidy DataFrame.
    Timestamps are Unix seconds, parsed as UTC then converted to the configured
    local timezone.

    clip_from / clip_to: tz-aware datetimes; when provided the result is sliced
    so the user's chosen date range is respected even if a broader window was
    fetched.
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

    if clip_from is not None:
        cf = (
            clip_from.astimezone(combined.index.tz)
            if combined.index.tz
            else clip_from.replace(tzinfo=None)
        )
        combined = combined[combined.index >= cf]
    if clip_to is not None:
        ct = (
            clip_to.astimezone(combined.index.tz)
            if combined.index.tz
            else clip_to.replace(tzinfo=None)
        )
        combined = combined[combined.index < ct]

    log.info(
        "Combined DataFrame: %d rows, index=%s, tz=%s",
        len(combined), type(combined.index).__name__,
        getattr(combined.index, 'tz', 'n/a'),
    )
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
    path  = Path(raw_path)
    files = sorted(path.glob("raw_*.parquet"))
    if not files:
        log.warning("No raw files found in %s", raw_path)
        return pd.DataFrame()
    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames).sort_index()
    df = df[~df.index.duplicated(keep='last')]
    df = _restore_datetime_index(df)
    log.info(
        "Loaded %d raw records from %d file(s), index=%s, tz=%s",
        len(df), len(files), type(df.index).__name__,
        getattr(df.index, 'tz', 'n/a'),
    )
    return df


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_ingest(cfg: dict, date_from: str = None, date_to: str = None) -> tuple:
    """
    Pull history and store as raw Parquet.

    Primary path (production):
        Direct InfluxDB query, 7-day chunks, driven by INFLUX_URL / INFLUX_TOKEN
        / INFLUX_ORG / INFLUX_BUCKET env vars (injected by the Portainer stack).

    Fallback path (dev / env vars absent):
        mqtt-bridge /history endpoint — identical to the original behaviour.

    date_from / date_to: optional YYYY-MM-DD strings from the UI that override
        history_range in settings.yaml.
    """
    mqtt_cfg  = cfg.get('mqtt', {})
    topic_map = mqtt_cfg.get('topics', {})
    sim_cfg   = cfg.get('simulation', {})
    window    = sim_cfg.get('history_window', '1m')
    tz        = sim_cfg.get('timezone', 'UTC')
    raw_path  = cfg['storage']['raw_path']

    short_to_label = {v: k for k, v in topic_map.items()}
    topics = list(topic_map.values())

    # --- Resolve date range ---
    clip_from = clip_to = None
    if date_from and date_to:
        range_str, clip_from, clip_to = dates_to_range_str(date_from, date_to)
        if range_str is None:
            range_str = sim_cfg.get('history_range', '700d')
        log.info(
            "User-supplied date range: %s -> %s  (requesting %s)",
            date_from, date_to, range_str,
        )
    else:
        range_str = sim_cfg.get('history_range', '700d')
        log.info("Using config history_range: %s", range_str)

    # Convert to absolute datetimes for the direct InfluxDB path
    now = datetime.now(timezone.utc)
    if clip_from and clip_to:
        abs_from = clip_from
        abs_to   = clip_to
    else:
        try:
            days = int(range_str.rstrip('d'))
        except ValueError:
            days = 700
        abs_from = now - timedelta(days=days)
        abs_to   = now

    # -----------------------------------------------------------------------
    # PRIMARY: Direct InfluxDB
    # -----------------------------------------------------------------------
    influx_env = _influx_env()
    if influx_env:
        log.info("Using DIRECT InfluxDB path (url=%s)", influx_env['url'])
        series = fetch_history_direct(
            influx_env=influx_env,
            topics=topics,
            dt_from=abs_from,
            dt_to=abs_to,
            window=window,
            chunk_days=7,
        )
        if series and any(series.values()):
            df = series_to_dataframe(
                series, short_to_label, tz=tz,
                clip_from=clip_from, clip_to=clip_to,
            )
            save_raw(df, raw_path)
            return df, {
                'ok': True,
                'reason': 'ok (direct InfluxDB)',
                'influx_enabled': True,
                'mqtt_connected': None,
                'rows': len(df),
                'topics_found': [t for t, v in series.items() if v],
                'date_from': str(df.index.min()) if not df.empty else '',
                'date_to':   str(df.index.max()) if not df.empty else '',
            }

        log.warning("Direct InfluxDB returned no data")
        return pd.DataFrame(), {
            'ok': False,
            'reason': (
                f"Direct InfluxDB query returned no data for topics {topics} "
                f"over {abs_from.date()} -> {abs_to.date()}. "
                "Check the bucket name, measurement names, and that data exists "
                "in this range."
            ),
            'influx_enabled': True,
            'mqtt_connected': None,
        }

    # -----------------------------------------------------------------------
    # FALLBACK: mqtt-bridge
    # -----------------------------------------------------------------------
    api_url = mqtt_cfg.get('api_url', '')
    log.info(
        "INFLUX_URL/INFLUX_TOKEN not set — falling back to mqtt-bridge at %s",
        api_url,
    )

    diag = check_bridge(api_url)
    if not diag['ok']:
        log.error("Bridge check failed: %s", diag['reason'])
        return pd.DataFrame(), diag

    log.info(
        "Fetching history via bridge: topics=%s range=%s window=%s tz=%s",
        topics, range_str, window, tz,
    )
    series = fetch_history(api_url, topics, range_str, window)

    if not series:
        return pd.DataFrame(), {
            'ok': False,
            'reason': (
                f"mqtt-bridge returned no data for topics {topics} "
                f"over range={range_str}. "
                "Check InfluxDB has data — the bridge must have been running "
                "long enough to record readings."
            ),
            'influx_enabled': True,
            'mqtt_connected': True,
        }

    df = series_to_dataframe(
        series, short_to_label, tz=tz,
        clip_from=clip_from, clip_to=clip_to,
    )
    save_raw(df, raw_path)

    diag['rows']         = len(df)
    diag['topics_found'] = list(series.keys())
    diag['date_from']    = str(df.index.min())
    diag['date_to']      = str(df.index.max())
    return df, diag


# ---------------------------------------------------------------------------
# Octopus personal account consumption path
# ---------------------------------------------------------------------------

def has_octopus_account(cfg: dict) -> bool:
    """
    Return True when all three required Octopus account fields are present
    in the config.  Export meter fields are optional.
    """
    acct = cfg.get("octopus_account", {})
    return bool(
        acct.get("api_key", "").strip()
        and acct.get("import_mpan", "").strip()
        and acct.get("import_serial", "").strip()
    )


def run_octopus_ingest(
    cfg: dict,
    date_from: str = None,
    date_to: str = None,
) -> tuple:
    """
    Fetch half-hourly consumption from the Octopus personal API and return
    an aggregated DataFrame ready for simulate.py.

    Because Octopus already returns data in 30-minute kWh intervals, this
    path bypasses the raw → aggregate step and writes directly to the
    aggregated store.

    Returns (agg_df, diag) where diag includes data_source='octopus_api'.
    On failure returns (empty DataFrame, diag with ok=False).
    """
    from app import octopus_client as _octo
    from app.aggregate import save_aggregated

    acct       = cfg.get("octopus_account", {})
    api_key    = acct.get("api_key", "").strip()
    imp_mpan   = acct.get("import_mpan", "").strip()
    imp_serial = acct.get("import_serial", "").strip()
    exp_mpan   = acct.get("export_mpan", "").strip()
    exp_serial = acct.get("export_serial", "").strip()
    tz         = cfg.get("simulation", {}).get("timezone", "UTC")
    agg_path   = cfg["storage"]["aggregated_path"]

    # --- Resolve date range ---
    now = datetime.now(timezone.utc)
    if date_from and date_to:
        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            dt_to   = datetime.strptime(date_to,   "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        except ValueError:
            dt_from = now - timedelta(days=365)
            dt_to   = now
    else:
        history_range = cfg.get("simulation", {}).get("history_range", "700d")
        try:
            days = int(history_range.rstrip("d"))
        except ValueError:
            days = 365
        dt_from = now - timedelta(days=days)
        dt_to   = now

    log.info(
        "Octopus ingest: mpan=%s serial=%s range=%s->%s",
        imp_mpan, imp_serial,
        dt_from.strftime("%Y-%m-%d"), dt_to.strftime("%Y-%m-%d"),
    )

    # --- Fetch import consumption ---
    try:
        import_records = _octo.get_consumption(imp_mpan, imp_serial, api_key, dt_from, dt_to)
    except Exception as e:
        log.error("Octopus import consumption fetch failed: %s", e)
        return pd.DataFrame(), {
            "ok": False,
            "reason": f"Octopus API error (import): {e}",
            "data_source": "octopus_api",
        }

    if not import_records:
        return pd.DataFrame(), {
            "ok": False,
            "reason": "Octopus API returned no import consumption for the requested period.",
            "data_source": "octopus_api",
        }

    # --- Fetch export consumption (optional) ---
    export_records = []
    has_export = bool(exp_mpan and exp_serial)
    if has_export:
        try:
            export_records = _octo.get_consumption(exp_mpan, exp_serial, api_key, dt_from, dt_to)
            log.info("Octopus export consumption: %d records", len(export_records))
        except Exception as e:
            log.warning("Octopus export consumption fetch failed (continuing without): %s", e)
            has_export = False

    # --- Build import DataFrame ---
    imp_df = pd.DataFrame(import_records)
    imp_df["interval_start"] = pd.to_datetime(imp_df["interval_start"], utc=True).dt.tz_convert(tz)
    imp_df = imp_df.set_index("interval_start")[["consumption"]].rename(
        columns={"consumption": "grid_import_kwh"}
    )

    # --- Build export DataFrame ---
    if export_records:
        exp_df = pd.DataFrame(export_records)
        exp_df["interval_start"] = pd.to_datetime(exp_df["interval_start"], utc=True).dt.tz_convert(tz)
        exp_df = exp_df.set_index("interval_start")[["consumption"]].rename(
            columns={"consumption": "grid_export_kwh"}
        )
        agg_df = imp_df.join(exp_df, how="left")
        agg_df["grid_export_kwh"] = agg_df["grid_export_kwh"].fillna(0.0)
    else:
        agg_df = imp_df.copy()
        agg_df["grid_export_kwh"] = 0.0

    agg_df = agg_df.sort_index()
    agg_df.index.name = "interval_start"

    save_aggregated(agg_df, agg_path, tag="octopus_latest")

    log.info(
        "Octopus ingest complete: %d intervals, has_export=%s, %s->%s",
        len(agg_df), has_export,
        str(agg_df.index.min().date()), str(agg_df.index.max().date()),
    )

    return agg_df, {
        "ok":          True,
        "reason":      "ok (Octopus personal API)",
        "data_source": "octopus_api",
        "has_export":  has_export,
        "intervals":   len(agg_df),
        "date_from":   str(agg_df.index.min().date()),
        "date_to":     str(agg_df.index.max().date()),
    }
