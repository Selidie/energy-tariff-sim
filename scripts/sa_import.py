#!/usr/bin/env python3
"""
sa_import.py — Import Solar Assistant backup (InfluxDB 1.x TSM shards) into mqtt-bridge InfluxDB 2.x.

Strategy:
  1. Extract the backup zip into a staging directory in the format influxd restore expects
  2. Start a blank InfluxDB 1.x container (no data mount)
  3. Run `influxd restore -portable -db solar_assistant <staging>` inside the container
  4. Query the restored data and write it into InfluxDB 2.x

Requirements:
  - Docker available on this host (uses 'docker' CLI)
  - influxdb-client Python package  (pip install influxdb-client --break-system-packages)
  - INFLUX_URL, INFLUX_TOKEN env vars set (same as mqtt-bridge .env)

Usage:
  # Inspect tags/fields to verify the restore worked (run this first)
  python3 sa_import.py /path/to/backup.zip --inspect

  # Dry-run — shows what would be written, no InfluxDB writes
  python3 sa_import.py /path/to/backup.zip --dry-run

  # Real import
  INFLUX_URL=http://localhost:8086 \
  INFLUX_TOKEN=your_token \
  python3 sa_import.py /path/to/backup.zip

Options:
  --dry-run          Preview topic mapping without writing to InfluxDB 2.x
  --inspect          Print tags and sample values for each measurement, then exit
  --range-start      ISO datetime — only import data after this point
  --range-end        ISO datetime — only import data before this point
  --prefix           MQTT prefix tag written to InfluxDB (default: solar_assistant)
  --batch-size       Points per InfluxDB 2.x write batch (default: 500)
  --v1-port          Host port for the temporary InfluxDB 1.x container (default: 18086)
  --keep-container   Don't remove the temporary container after import

Environment variables:
  INFLUX_URL         Your InfluxDB 2.x URL    e.g. http://localhost:8086
  INFLUX_TOKEN       Your InfluxDB 2.x token
  INFLUX_ORG         Your InfluxDB 2.x org    (default: home)
  INFLUX_BUCKET      Your InfluxDB 2.x bucket (default: solar)
"""

import os
import sys
import io
import time
import json
import shutil
import zipfile
import tarfile
import tempfile
import argparse
import logging
import subprocess
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# InfluxDB 2.x config (mirrors mqtt-bridge .env)
# ---------------------------------------------------------------------------
INFLUX_URL    = os.environ.get('INFLUX_URL', '').strip()
INFLUX_TOKEN  = os.environ.get('INFLUX_TOKEN', '').strip()
INFLUX_ORG    = os.environ.get('INFLUX_ORG', 'home')
INFLUX_BUCKET = os.environ.get('INFLUX_BUCKET', 'solar')

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SA_DB_NAME     = 'solar_assistant'
CONTAINER_NAME = 'sa_import_influx1'
V1_IMAGE       = 'influxdb:1.8'
STAGING_MOUNT  = '/tmp/sa_backup'

# ---------------------------------------------------------------------------
# Solar Assistant measurement → (mqtt topic, preferred field name)
#
# Solar Assistant stores data in two field types:
#   'combined'   — aggregate/total value across all inverters
#   'inverter_0' — per-inverter value (first/only inverter)
#
# For 'total/...' topics we prefer 'combined'.
# For 'inverter_1/...' topics we prefer 'inverter_0'.
# Fallback: use whatever field is present if the preferred one isn't.
# ---------------------------------------------------------------------------
MEASUREMENT_MAP = {
    # measurement name                 topic path                              preferred field
    'AC output voltage':        ('inverter_1/ac_output_voltage/state',        'inverter_0'),
    'Battery current':          ('battery_1/current/state',                   'inverter_0'),
    'Battery power':            ('total/battery_power/state',                 'combined'),
    'Battery power hourly':     ('total/battery_power_hourly/state',          'combined'),
    'Battery power in hourly':  ('total/battery_power_in_hourly/state',       'combined'),
    'Battery power out hourly': ('total/battery_power_out_hourly/state',      'combined'),
    'Battery state of charge':  ('total/battery_state_of_charge/state',       'combined'),
    'Battery temperature':      ('battery_1/temperature/state',               'combined'),
    'Battery voltage':          ('battery_1/voltage/state',                   'inverter_0'),
    'Cloud cover':              ('weather/cloud_cover/state',                 'combined'),
    'Generator power':          ('total/generator_power/state',               'inverter_0'),
    'Grid frequency':           ('inverter_1/grid_frequency/state',           'inverter_0'),
    'Grid power':               ('total/grid_power/state',                    'combined'),
    'Grid power hourly':        ('total/grid_power_hourly/state',             'combined'),
    'Grid power in hourly':     ('total/grid_power_in_hourly/state',          'combined'),
    'Grid power out hourly':    ('total/grid_power_out_hourly/state',         'combined'),
    'Grid voltage':             ('inverter_1/grid_voltage/state',             'inverter_0'),
    'Inverter temperature':     ('inverter_1/temperature/state',              'inverter_0'),
    'Load power':               ('total/load_power/state',                    'combined'),
    'Load power essential':     ('total/load_power_essential/state',          'inverter_0'),
    'Load power hourly':        ('total/load_power_hourly/state',             'combined'),
    'Load power non-essential': ('total/load_power_non_essential/state',      'inverter_0'),
    'Outside temperature':      ('weather/outside_temperature/state',         'combined'),
    'PV current 1':             ('inverter_1/pv_current_1/state',             'inverter_0'),
    'PV current 2':             ('inverter_1/pv_current_2/state',             'inverter_0'),
    'PV power':                 ('total/pv_power/state',                      'combined'),
    'PV power 1':               ('inverter_1/pv_power_1/state',               'inverter_0'),
    'PV power 2':               ('inverter_1/pv_power_2/state',               'inverter_0'),
    'PV power hourly':          ('total/pv_power_hourly/state',               'combined'),
    'PV power predicted':       ('weather/pv_power_predicted/state',          'combined'),
    'PV power predicted hourly':('weather/pv_power_predicted_hourly/state',   'combined'),
    'PV voltage 1':             ('inverter_1/pv_voltage_1/state',             'inverter_0'),
    'PV voltage 2':             ('inverter_1/pv_voltage_2/state',             'inverter_0'),
}


# ---------------------------------------------------------------------------
# Step 1 — Extract backup zip into a flat staging directory
# ---------------------------------------------------------------------------
def extract_backup_to_staging(backup_path: str, tmp_dir: str) -> str:
    """
    Extract the Solar Assistant backup zip into a flat staging directory
    that influxd restore -portable can consume directly.

    The zip contains: <timestamp>.meta, <timestamp>.manifest, <timestamp>.s<N>.tar.gz
    influxd restore -portable expects all of these flat in one directory.
    The .tar.gz shard files must NOT be pre-extracted.
    """
    staging_dir = os.path.join(tmp_dir, 'backup_staging')
    os.makedirs(staging_dir, exist_ok=True)

    log.info('Staging backup files from: %s', backup_path)

    with zipfile.ZipFile(backup_path, 'r') as zf:
        members = zf.namelist()
        log.info('Found %d files in zip', len(members))
        for name in members:
            dest = os.path.join(staging_dir, os.path.basename(name))
            with open(dest, 'wb') as f:
                f.write(zf.read(name))

    for dirpath, dirnames, filenames in os.walk(staging_dir):
        os.chmod(dirpath, 0o755)
        for fname in filenames:
            os.chmod(os.path.join(dirpath, fname), 0o644)

    counts = {}
    for f in os.listdir(staging_dir):
        ext = f.rsplit('.', 1)[-1] if '.' in f else 'other'
        counts[ext] = counts.get(ext, 0) + 1
    log.info('Staged: %s', ', '.join(f'{v} .{k}' for k, v in sorted(counts.items())))

    return staging_dir


# ---------------------------------------------------------------------------
# Step 2 — Start a blank InfluxDB 1.x container with staging dir mounted
# ---------------------------------------------------------------------------
def start_v1_container(staging_dir: str, host_port: int) -> str:
    subprocess.run(['docker', 'rm', '-f', CONTAINER_NAME], capture_output=True)

    log.info('Pulling InfluxDB 1.8 image (if not cached)...')
    subprocess.run(['docker', 'pull', V1_IMAGE], check=True)

    cmd = [
        'docker', 'run', '-d',
        '--name', CONTAINER_NAME,
        '-p', f'{host_port}:8086',
        '-v', f'{staging_dir}:{STAGING_MOUNT}:ro',
        '-e', 'INFLUXDB_HTTP_AUTH_ENABLED=false',
        V1_IMAGE
    ]
    log.info('Starting blank InfluxDB 1.x container on port %d...', host_port)
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    container_id = result.stdout.strip()
    log.info('Container started: %s', container_id[:12])
    return container_id


def wait_for_v1(host_port: int, timeout: int = 90):
    url      = f'http://localhost:{host_port}/ping'
    deadline = time.time() + timeout
    log.info('Waiting for InfluxDB 1.x to be ready...')
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            log.info('InfluxDB 1.x is ready.')
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f'InfluxDB 1.x did not start within {timeout}s')


# ---------------------------------------------------------------------------
# Step 3 — Run influxd restore inside the container
# ---------------------------------------------------------------------------
def run_restore(host_port: int):
    """
    Use influxd restore -portable to load the backup correctly.
    This registers shard groups in meta.db AND copies TSM data,
    making everything immediately queryable.
    Tries the live RPC restore first; falls back to offline if needed.
    """
    log.info('Running influxd restore -portable inside container...')

    cmd = [
        'docker', 'exec', CONTAINER_NAME,
        'influxd', 'restore',
        '-portable',
        '-host', 'localhost:8088',
        '-db', SA_DB_NAME,
        STAGING_MOUNT
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    log.info('restore stdout: %s', result.stdout.strip())
    if result.stderr.strip():
        log.info('restore stderr: %s', result.stderr.strip())

    if result.returncode != 0:
        log.warning('RPC restore failed (returncode=%d), trying offline restore...', result.returncode)
        _offline_restore(host_port)
    else:
        log.info('Restore completed successfully via RPC.')
        time.sleep(5)


def _offline_restore(host_port: int):
    log.info('Stopping influxd inside container for offline restore...')
    subprocess.run(['docker', 'exec', CONTAINER_NAME, 'pkill', 'influxd'], capture_output=True)
    time.sleep(3)

    result = subprocess.run(
        ['docker', 'exec', CONTAINER_NAME, 'influxd', 'restore', '-portable',
         '-db', SA_DB_NAME, STAGING_MOUNT],
        capture_output=True, text=True
    )
    log.info('restore stdout: %s', result.stdout.strip())
    if result.stderr.strip():
        log.info('restore stderr: %s', result.stderr.strip())
    if result.returncode != 0:
        raise RuntimeError(f'influxd restore failed:\n{result.stderr}')

    subprocess.run(['docker', 'restart', CONTAINER_NAME], check=True, capture_output=True)
    wait_for_v1(host_port)
    time.sleep(5)


def stop_v1_container():
    subprocess.run(['docker', 'rm', '-f', CONTAINER_NAME], capture_output=True)
    log.info('Temporary container removed.')


def check_container_logs(lines: int = 10):
    result = subprocess.run(
        ['docker', 'logs', '--tail', str(lines), CONTAINER_NAME],
        capture_output=True, text=True
    )
    log.info('--- Container logs (tail %d) ---\n%s%s', lines, result.stdout, result.stderr)


# ---------------------------------------------------------------------------
# Step 4 — Query InfluxDB 1.x
# ---------------------------------------------------------------------------
def v1_query(host_port: int, query: str, db: str = None, epoch: str = None) -> dict:
    params = {'q': query}
    if db:
        params['db'] = db
    if epoch:
        params['epoch'] = epoch
    url = f'http://localhost:{host_port}/query?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.loads(resp.read())


def get_measurements(host_port: int) -> list:
    data   = v1_query(host_port, f'SHOW MEASUREMENTS ON "{SA_DB_NAME}"')
    series = data.get('results', [{}])[0].get('series', [])
    if not series:
        return []
    return [row[0] for row in series[0].get('values', [])]


def get_field_keys(host_port: int, measurement: str) -> list:
    """Return unique field key names for the measurement."""
    data   = v1_query(host_port, f'SHOW FIELD KEYS FROM "{measurement}"', db=SA_DB_NAME)
    series = data.get('results', [{}])[0].get('series', [])
    if not series:
        return []
    # deduplicate while preserving order
    seen = set()
    keys = []
    for row in series[0].get('values', []):
        k = row[0]
        if k not in seen:
            seen.add(k)
            keys.append(k)
    return keys


def pick_field(available_fields: list, preferred: str) -> str:
    """
    Return the preferred field if present, otherwise fall back to the first
    available field.  Returns None if no fields at all.
    """
    if not available_fields:
        return None
    if preferred in available_fields:
        return preferred
    log.warning('Preferred field "%s" not found, using "%s" instead',
                preferred, available_fields[0])
    return available_fields[0]


def inspect_measurements(host_port: int):
    measurements = get_measurements(host_port)
    log.info('Found %d measurements', len(measurements))

    print('\n' + '='*70)
    print('MEASUREMENT INSPECTION')
    print('='*70)
    for m in measurements:
        fields  = get_field_keys(host_port, m)
        mapping = MEASUREMENT_MAP.get(m)

        if mapping:
            topic, preferred = mapping
            chosen_field = pick_field(fields, preferred)
        else:
            topic        = '*** NOT MAPPED ***'
            chosen_field = fields[0] if fields else None

        try:
            count_data   = v1_query(host_port, f'SELECT COUNT("{chosen_field}") FROM "{m}"',
                                    db=SA_DB_NAME, epoch='ns')
            count_series = count_data.get('results', [{}])[0].get('series', [])
            row_count    = count_series[0]['values'][0][1] if count_series else 0
        except Exception:
            row_count = '?'

        print(f'\n  Measurement  : {m}')
        print(f'  → topic      : {topic}')
        print(f'  All fields   : {fields}')
        print(f'  Import field : {chosen_field}')
        print(f'  Row count    : {row_count}')
    print('\n' + '='*70 + '\n')


def stream_v1_measurement(host_port: int, measurement: str, field: str,
                           range_start=None, range_end=None, page_size: int = 50000):
    """
    Generator yielding (timestamp_ns, float_value) for the specified field only.
    Uses LIMIT/OFFSET pagination to handle large datasets without loading all into memory.
    """
    conditions = []
    if range_start:
        conditions.append(f"time >= '{range_start.strftime('%Y-%m-%dT%H:%M:%SZ')}'")
    if range_end:
        conditions.append(f"time <= '{range_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'")
    where = f' WHERE {" AND ".join(conditions)}' if conditions else ''

    offset = 0
    while True:
        # SELECT only the specific field we want — avoids pulling unwanted columns
        query  = (f'SELECT "{field}" FROM "{measurement}"{where} '
                  f'ORDER BY time ASC LIMIT {page_size} OFFSET {offset}')
        params = {'db': SA_DB_NAME, 'q': query, 'epoch': 'ns'}
        url    = f'http://localhost:{host_port}/query?' + urllib.parse.urlencode(params)

        with urllib.request.urlopen(url, timeout=120) as resp:
            data = json.loads(resp.read())

        series = data.get('results', [{}])[0].get('series', [])
        if not series:
            break

        rows_this_page = 0
        for s in series:
            cols = s.get('columns', [])   # ['time', '<field>']
            try:
                val_idx = cols.index(field)
            except ValueError:
                continue
            for row in s.get('values', []):
                rows_this_page += 1
                ts_ns = row[0]
                val   = row[val_idx]
                if val is None or ts_ns is None:
                    continue
                try:
                    yield ts_ns, float(val)
                except (ValueError, TypeError):
                    continue

        if rows_this_page < page_size:
            break
        offset += page_size


# ---------------------------------------------------------------------------
# Step 5 — Write to InfluxDB 2.x
# ---------------------------------------------------------------------------
def run_import(host_port: int, prefix: str, range_start, range_end,
               dry_run: bool, batch_size: int, write_api, influx_org, influx_bucket):

    from influxdb_client import Point

    measurements = get_measurements(host_port)
    log.info('Found %d measurements to import', len(measurements))

    total   = 0
    skipped = 0
    written = 0
    batch   = []
    preview = 0

    for measurement in measurements:
        mapping = MEASUREMENT_MAP.get(measurement)
        if not mapping:
            log.warning('No topic mapping for measurement "%s" — skipping', measurement)
            skipped += 1
            continue

        short_topic, preferred_field = mapping
        available_fields = get_field_keys(host_port, measurement)
        field = pick_field(available_fields, preferred_field)
        if not field:
            log.warning('No fields found for "%s" — skipping', measurement)
            skipped += 1
            continue

        log.info('Importing: %-35s field=%-12s → %s', measurement, field, short_topic)

        for ts_ns, float_val in stream_v1_measurement(
                host_port, measurement, field, range_start, range_end):

            total += 1
            ts_utc = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)

            if dry_run:
                if preview < 40:
                    log.info('[DRY-RUN] topic=%-55s value=%-12.4f  time=%s',
                             short_topic, float_val, ts_utc.isoformat())
                    preview += 1
                elif preview == 40:
                    log.info('[DRY-RUN] (further previews suppressed — counting only)')
                    preview += 1
                continue

            point = (
                Point('solar')
                .tag('topic',  short_topic)
                .tag('prefix', prefix)
                .field('value', float_val)
                .time(ts_utc)
            )
            batch.append(point)

            if len(batch) >= batch_size:
                write_api.write(bucket=influx_bucket, org=influx_org, record=batch)
                written += len(batch)
                batch = []
                if written % 10000 == 0:
                    log.info('  Written %d points so far...', written)

    if batch and not dry_run:
        write_api.write(bucket=influx_bucket, org=influx_org, record=batch)
        written += len(batch)

    return total, skipped, written


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description='Import Solar Assistant InfluxDB 1.x backup into InfluxDB 2.x'
    )
    parser.add_argument('backup',           help='Path to Solar Assistant backup .zip')
    parser.add_argument('--dry-run',        action='store_true', help='Preview without writing')
    parser.add_argument('--inspect',        action='store_true', help='Print field/count info and exit')
    parser.add_argument('--range-start',    help='Import from this ISO datetime')
    parser.add_argument('--range-end',      help='Import up to this ISO datetime')
    parser.add_argument('--prefix',         default='solar_assistant')
    parser.add_argument('--batch-size',     type=int, default=500)
    parser.add_argument('--v1-port',        type=int, default=18086)
    parser.add_argument('--keep-container', action='store_true')
    args = parser.parse_args()

    range_start = datetime.fromisoformat(args.range_start).replace(tzinfo=timezone.utc) if args.range_start else None
    range_end   = datetime.fromisoformat(args.range_end).replace(tzinfo=timezone.utc)   if args.range_end   else None

    write_api = None
    if not args.dry_run and not args.inspect:
        if not INFLUX_URL or not INFLUX_TOKEN:
            log.error('INFLUX_URL and INFLUX_TOKEN must be set (or use --dry-run / --inspect)')
            sys.exit(1)
        try:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS
            client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
            write_api = client.write_api(write_options=SYNCHRONOUS)
            log.info('Connected to InfluxDB 2.x: %s  bucket=%s  org=%s',
                     INFLUX_URL, INFLUX_BUCKET, INFLUX_ORG)
        except Exception as e:
            log.error('Failed to connect to InfluxDB 2.x: %s', e)
            sys.exit(1)
    elif args.dry_run:
        log.info('*** DRY-RUN MODE — nothing will be written ***')

    tmp_dir      = tempfile.mkdtemp(prefix='sa_import_')
    container_id = None

    try:
        staging_dir  = extract_backup_to_staging(args.backup, tmp_dir)
        container_id = start_v1_container(staging_dir, args.v1_port)
        wait_for_v1(args.v1_port)
        run_restore(args.v1_port)
        check_container_logs(10)

        if args.inspect:
            inspect_measurements(args.v1_port)
            return

        total, skipped, written = run_import(
            args.v1_port, args.prefix, range_start, range_end,
            args.dry_run, args.batch_size,
            write_api, INFLUX_ORG, INFLUX_BUCKET
        )

        log.info('--- Import complete ---')
        if args.dry_run:
            log.info('Would have written : %d points  (unmapped measurements skipped: %d)', total, skipped)
        else:
            log.info('Written  : %d points', written)
            log.info('Skipped  : %d unmapped measurements', skipped)

    finally:
        if container_id and not args.keep_container:
            stop_v1_container()
        elif args.keep_container:
            log.info('Container left running: %s (port %d)', CONTAINER_NAME, args.v1_port)
        try:
            shutil.rmtree(tmp_dir)
            log.info('Temp dir cleaned up.')
        except Exception:
            pass


if __name__ == '__main__':
    main()
