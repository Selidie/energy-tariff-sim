#!/usr/bin/env python3
"""
sa_import.py — Import Solar Assistant backup (InfluxDB 1.x TSM shards) into mqtt-bridge InfluxDB 2.x.

Usage:
  python3 sa_import.py /path/to/backup.zip
  python3 sa_import.py /path/to/backup.zip --dry-run
  python3 sa_import.py /path/to/backup.zip --inspect
  python3 sa_import.py /path/to/backup.zip --range-start 2025-01-01T00:00:00

Options:
  --dry-run          Preview topic mapping without writing to InfluxDB 2.x
  --inspect          Print tags and sample values for each measurement, then exit
  --range-start      ISO datetime — only import data after this point
  --range-end        ISO datetime — only import data before this point
  --prefix           MQTT prefix tag written to InfluxDB (default: solar_assistant)
  --batch-size       Points per InfluxDB 2.x write batch (default: 200)
  --write-pause      Seconds to sleep between batches (default: 0.0)
  --docker-network   Docker network the temporary container joins (default: frontend)
  --keep-container   Don't remove the temporary container after import

Environment variables:
  INFLUX_URL             InfluxDB 2.x URL         e.g. http://influxdb:8086
  INFLUX_TOKEN           InfluxDB 2.x token
  INFLUX_ORG             InfluxDB 2.x org          (default: home)
  INFLUX_BUCKET          InfluxDB 2.x bucket       (default: solar)
  DOCKER_NETWORK         Docker network (default: frontend; dev uses dev-network)
  STAGING_CONTAINER_DIR  Path inside this container for staging files
                         (default: /app/data/sa_staging)
  STAGING_HOST_DIR       Override: corresponding host path for Docker bind-mount.
                         If unset, auto-resolved at runtime by inspecting this
                         container's own mounts via `docker inspect`.
"""

import os
import sys
import time
import json
import shutil
import socket
import zipfile
import argparse
import logging
import subprocess
import urllib.request
import urllib.parse
import concurrent.futures
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

INFLUX_URL    = os.environ.get('INFLUX_URL', '').strip()
INFLUX_TOKEN  = os.environ.get('INFLUX_TOKEN', '').strip()
INFLUX_ORG    = os.environ.get('INFLUX_ORG', 'home')
INFLUX_BUCKET = os.environ.get('INFLUX_BUCKET', 'solar')

SA_DB_NAME       = 'solar_assistant'
CONTAINER_NAME   = 'sa_import_influx1'
V1_IMAGE         = 'influxdb:1.8'
STAGING_MOUNT    = '/tmp/sa_backup'
V1_INTERNAL_PORT = 8086

# Resource limits for the temporary InfluxDB 1.x sidecar container.
# Capped to 1 CPU and 2 GB RAM so the import can never starve the host,
# regardless of how many cores or how much memory the deployment has.
# The import will take longer but remain stable on constrained homelabs.
V1_CPU_LIMIT    = '1'
V1_MEMORY_LIMIT = '2g'

# Rows fetched per paginated query from InfluxDB 1.x.
# Smaller pages mean less memory pressure inside the sidecar at any one time.
V1_PAGE_SIZE = 10000

DEFAULT_DOCKER_NETWORK        = os.environ.get('DOCKER_NETWORK', 'frontend')
DEFAULT_STAGING_CONTAINER_DIR = os.environ.get('STAGING_CONTAINER_DIR', '/app/data/sa_staging')

WRITE_TIMEOUT_SECS = 60
WRITE_RETRIES      = 3
WRITE_RETRY_DELAY  = 10

# Topic names must match exactly what the live mqtt-bridge records in InfluxDB.
# The bridge strips the MQTT prefix (e.g. "solar_assistant/") and stores the
# remainder as the 'topic' tag.  Solar Assistant publishes per-inverter readings
# on inverter_1/... topics, so historical imports must use the same names.
MEASUREMENT_MAP = {
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
    'Grid power':               ('inverter_1/grid_power/state',               'inverter_0'),  # matches live mqtt-bridge topic
    'Grid power hourly':        ('inverter_1/grid_power_hourly/state',        'inverter_0'),
    'Grid power in hourly':     ('inverter_1/grid_power_in_hourly/state',     'inverter_0'),
    'Grid power out hourly':    ('inverter_1/grid_power_out_hourly/state',    'inverter_0'),
    'Grid voltage':             ('inverter_1/grid_voltage/state',             'inverter_0'),
    'Inverter temperature':     ('inverter_1/temperature/state',              'inverter_0'),
    'Load power':               ('inverter_1/load_power/state',               'inverter_0'),  # matches live mqtt-bridge topic
    'Load power essential':     ('total/load_power_essential/state',          'inverter_0'),
    'Load power hourly':        ('inverter_1/load_power_hourly/state',        'inverter_0'),
    'Load power non-essential': ('total/load_power_non_essential/state',      'inverter_0'),
    'Outside temperature':      ('weather/outside_temperature/state',         'combined'),
    'PV current 1':             ('inverter_1/pv_current_1/state',             'inverter_0'),
    'PV current 2':             ('inverter_1/pv_current_2/state',             'inverter_0'),
    'PV power':                 ('inverter_1/pv_power/state',                 'inverter_0'),  # matches live mqtt-bridge topic
    'PV power 1':               ('inverter_1/pv_power_1/state',               'inverter_0'),
    'PV power 2':               ('inverter_1/pv_power_2/state',               'inverter_0'),
    'PV power hourly':          ('inverter_1/pv_power_hourly/state',          'inverter_0'),
    'PV power predicted':       ('weather/pv_power_predicted/state',          'combined'),
    'PV power predicted hourly':('weather/pv_power_predicted_hourly/state',   'combined'),
    'PV voltage 1':             ('inverter_1/pv_voltage_1/state',             'inverter_0'),
    'PV voltage 2':             ('inverter_1/pv_voltage_2/state',             'inverter_0'),
}


# ---------------------------------------------------------------------------
# Host-path resolution
# ---------------------------------------------------------------------------

def resolve_host_path(container_path: str) -> str:
    """
    Translate a path that exists inside this container into the equivalent
    path on the Docker host by inspecting our own container's mount table.

    This is needed because sa_import.py spins up a sibling InfluxDB 1.x
    container via the Docker socket — a Docker-out-of-Docker pattern.  The
    bind-mount source passed to `docker run -v` must be a HOST path, not a
    path inside this container's filesystem.  Since the host layout varies
    across deployments (dev, staging, production), we resolve it dynamically
    rather than hard-coding or requiring an env var.

    Strategy:
      1. Use the container's hostname (== short container ID by default) to
         look up our own container via `docker inspect`.
      2. Walk the Mounts list to find the entry whose `Destination` is a
         prefix of `container_path`.
      3. Re-root the path under the mount's `Source` (host path).

    Falls back to returning `container_path` unchanged if:
      - docker inspect fails (e.g. running outside Docker, unit tests)
      - no matching mount is found (path is in an un-mounted layer)
    """
    self_id = socket.gethostname()
    try:
        result = subprocess.run(
            ['docker', 'inspect', self_id],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            log.debug('docker inspect returned %d — cannot resolve host path', result.returncode)
            return container_path

        info = json.loads(result.stdout)
        if not info:
            return container_path

        mounts = info[0].get('Mounts', [])

        best_dest   = ''
        best_source = ''
        for m in mounts:
            dest      = m.get('Destination', '')
            dest_norm = dest.rstrip('/')
            if container_path == dest_norm or container_path.startswith(dest_norm + '/'):
                if len(dest_norm) > len(best_dest):
                    best_dest   = dest_norm
                    best_source = m.get('Source', '').rstrip('/')

        if not best_dest:
            log.warning(
                'No mount found covering container path %s — '
                'using container path as-is (may fail if Docker socket is host-mounted)',
                container_path
            )
            return container_path

        relative  = container_path[len(best_dest):]
        host_path = best_source + relative
        log.info('Resolved host path: %s → %s  (via mount %s → %s)',
                 container_path, host_path, best_dest, best_source)
        return host_path

    except Exception as e:
        log.warning('Could not resolve host path for %s: %s — using as-is', container_path, e)
        return container_path


def get_staging_host_dir(staging_container_dir: str) -> str:
    """
    Return the host-side path for the staging directory.

    Priority:
      1. STAGING_HOST_DIR env var (explicit override — useful for testing or
         unusual mount configurations)
      2. Auto-resolved via docker inspect of this container's own mounts
    """
    override = os.environ.get('STAGING_HOST_DIR', '').strip()
    if override:
        log.info('Using STAGING_HOST_DIR override: %s', override)
        return override
    return resolve_host_path(staging_container_dir)


# ---------------------------------------------------------------------------
# Backup extraction
# ---------------------------------------------------------------------------

def extract_backup_to_staging(backup_path: str, staging_container_dir: str) -> None:
    if os.path.exists(staging_container_dir):
        shutil.rmtree(staging_container_dir)
    os.makedirs(staging_container_dir, exist_ok=True)
    log.info('Staging backup from: %s → %s', backup_path, staging_container_dir)
    with zipfile.ZipFile(backup_path, 'r') as zf:
        members = zf.namelist()
        log.info('Found %d files in zip', len(members))
        for name in members:
            dest = os.path.join(staging_container_dir, os.path.basename(name))
            with open(dest, 'wb') as f:
                f.write(zf.read(name))
    for dirpath, dirnames, filenames in os.walk(staging_container_dir):
        os.chmod(dirpath, 0o755)
        for fname in filenames:
            os.chmod(os.path.join(dirpath, fname), 0o644)
    counts = {}
    for f in os.listdir(staging_container_dir):
        ext = f.rsplit('.', 1)[-1] if '.' in f else 'other'
        counts[ext] = counts.get(ext, 0) + 1
    log.info('Staged: %s', ', '.join(f'{v} .{k}' for k, v in sorted(counts.items())))


# ---------------------------------------------------------------------------
# InfluxDB 1.x sidecar container
# ---------------------------------------------------------------------------

def start_v1_container(staging_host_dir: str, docker_network: str) -> str:
    """
    Start a temporary InfluxDB 1.x container with explicit resource limits.

    CPU and memory are capped (V1_CPU_LIMIT / V1_MEMORY_LIMIT) so the sidecar
    cannot starve the host regardless of deployment size.  The import will run
    slower on constrained hardware but will remain stable and won't lock up
    other services sharing the same VM or node.
    """
    subprocess.run(['docker', 'rm', '-f', CONTAINER_NAME], capture_output=True)
    log.info('Pulling InfluxDB 1.8 image (if not cached)...')
    subprocess.run(['docker', 'pull', V1_IMAGE], check=True)
    cmd = [
        'docker', 'run', '-d',
        '--name',    CONTAINER_NAME,
        '--network', docker_network,
        '--cpus',    V1_CPU_LIMIT,
        '--memory',  V1_MEMORY_LIMIT,
        '-v', f'{staging_host_dir}:{STAGING_MOUNT}:ro',
        '-e', 'INFLUXDB_HTTP_AUTH_ENABLED=false',
        V1_IMAGE
    ]
    log.info('Starting InfluxDB 1.x container on network %s (cpus=%s memory=%s)...',
             docker_network, V1_CPU_LIMIT, V1_MEMORY_LIMIT)
    log.info('Bind-mount: %s → %s', staging_host_dir, STAGING_MOUNT)
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    container_id = result.stdout.strip()
    log.info('Container started: %s', container_id[:12])
    return container_id


def _v1_base_url() -> str:
    return f'http://{CONTAINER_NAME}:{V1_INTERNAL_PORT}'


def wait_for_v1(timeout: int = 90):
    url = f'{_v1_base_url()}/ping'
    deadline = time.time() + timeout
    log.info('Waiting for InfluxDB 1.x at %s ...', url)
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            log.info('InfluxDB 1.x is ready.')
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f'InfluxDB 1.x did not start within {timeout}s')


def run_restore():
    log.info('Running influxd restore -portable inside container...')
    cmd = [
        'docker', 'exec', CONTAINER_NAME,
        'influxd', 'restore', '-portable',
        '-host', 'localhost:8088',
        '-db', SA_DB_NAME, STAGING_MOUNT
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    log.info('restore stdout: %s', result.stdout.strip())
    if result.stderr.strip():
        log.info('restore stderr: %s', result.stderr.strip())
    if result.returncode != 0:
        log.warning('RPC restore failed (rc=%d), trying offline restore...', result.returncode)
        _offline_restore()
    else:
        log.info('Restore completed successfully.')
        time.sleep(5)


def _offline_restore():
    subprocess.run(['docker', 'exec', CONTAINER_NAME, 'pkill', 'influxd'], capture_output=True)
    time.sleep(3)
    result = subprocess.run(
        ['docker', 'exec', CONTAINER_NAME, 'influxd', 'restore', '-portable',
         '-db', SA_DB_NAME, STAGING_MOUNT],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f'influxd restore failed:\n{result.stderr}')
    subprocess.run(['docker', 'restart', CONTAINER_NAME], check=True, capture_output=True)
    wait_for_v1()
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
# InfluxDB 1.x queries
# ---------------------------------------------------------------------------

def v1_query(query: str, db: str = None, epoch: str = None) -> dict:
    params = {'q': query}
    if db:
        params['db'] = db
    if epoch:
        params['epoch'] = epoch
    url = f'{_v1_base_url()}/query?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.loads(resp.read())


def get_measurements() -> list:
    data = v1_query(f'SHOW MEASUREMENTS ON "{SA_DB_NAME}"')
    series = data.get('results', [{}])[0].get('series', [])
    if not series:
        return []
    return [row[0] for row in series[0].get('values', [])]


def get_field_keys(measurement: str) -> list:
    data = v1_query(f'SHOW FIELD KEYS FROM "{measurement}"', db=SA_DB_NAME)
    series = data.get('results', [{}])[0].get('series', [])
    if not series:
        return []
    seen, keys = set(), []
    for row in series[0].get('values', []):
        k = row[0]
        if k not in seen:
            seen.add(k)
            keys.append(k)
    return keys


def pick_field(available_fields: list, preferred: str) -> str:
    if not available_fields:
        return None
    if preferred in available_fields:
        return preferred
    log.warning('Preferred field "%s" not found, using "%s"', preferred, available_fields[0])
    return available_fields[0]


def inspect_measurements():
    measurements = get_measurements()
    log.info('Found %d measurements', len(measurements))
    print('\n' + '='*70)
    print('MEASUREMENT INSPECTION')
    print('='*70)
    for m in measurements:
        fields = get_field_keys(m)
        mapping = MEASUREMENT_MAP.get(m)
        if mapping:
            topic, preferred = mapping
            chosen_field = pick_field(fields, preferred)
        else:
            topic = '*** NOT MAPPED ***'
            chosen_field = fields[0] if fields else None
        try:
            count_data = v1_query(f'SELECT COUNT("{chosen_field}") FROM "{m}"', db=SA_DB_NAME, epoch='ns')
            count_series = count_data.get('results', [{}])[0].get('series', [])
            row_count = count_series[0]['values'][0][1] if count_series else 0
        except Exception:
            row_count = '?'
        print(f'\n  Measurement  : {m}')
        print(f'  → topic      : {topic}')
        print(f'  All fields   : {fields}')
        print(f'  Import field : {chosen_field}')
        print(f'  Row count    : {row_count}')
    print('\n' + '='*70 + '\n')


# ---------------------------------------------------------------------------
# Data streaming
# ---------------------------------------------------------------------------

def stream_v1_measurement(measurement: str, field: str,
                           range_start=None, range_end=None):
    """
    Generator yielding (timestamp_ns, float_value) for the given measurement.

    Uses LIMIT/OFFSET pagination with V1_PAGE_SIZE rows per request to keep
    memory pressure inside the sidecar low and predictable.  Smaller pages
    mean more round-trips but far less peak RAM usage, which is the right
    trade-off for constrained homelab deployments.
    """
    conditions = []
    if range_start:
        conditions.append(f"time >= '{range_start.strftime('%Y-%m-%dT%H:%M:%SZ')}'")
    if range_end:
        conditions.append(f"time <= '{range_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'")
    where = f' WHERE {" AND ".join(conditions)}' if conditions else ''
    offset = 0
    while True:
        query = (f'SELECT "{field}" FROM "{measurement}"{where} '
                 f'ORDER BY time ASC LIMIT {V1_PAGE_SIZE} OFFSET {offset}')
        params = {'db': SA_DB_NAME, 'q': query, 'epoch': 'ns'}
        url = f'{_v1_base_url()}/query?' + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=120) as resp:
            data = json.loads(resp.read())
        series = data.get('results', [{}])[0].get('series', [])
        if not series:
            break
        rows_this_page = 0
        for s in series:
            cols = s.get('columns', [])
            try:
                val_idx = cols.index(field)
            except ValueError:
                continue
            for row in s.get('values', []):
                rows_this_page += 1
                ts_ns, val = row[0], row[val_idx]
                if val is None or ts_ns is None:
                    continue
                try:
                    yield ts_ns, float(val)
                except (ValueError, TypeError):
                    continue
        if rows_this_page < V1_PAGE_SIZE:
            break
        offset += V1_PAGE_SIZE


# ---------------------------------------------------------------------------
# InfluxDB 2.x writes
# ---------------------------------------------------------------------------

def write_batch_with_retry(write_api, bucket: str, org: str, batch: list) -> bool:
    for attempt in range(1, WRITE_RETRIES + 1):
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(write_api.write, bucket=bucket, org=org, record=batch)
                future.result(timeout=WRITE_TIMEOUT_SECS)
            return True
        except concurrent.futures.TimeoutError:
            log.warning('Write attempt %d/%d timed out after %ds', attempt, WRITE_RETRIES, WRITE_TIMEOUT_SECS)
        except Exception as e:
            log.warning('Write attempt %d/%d failed: %s', attempt, WRITE_RETRIES, e)
        if attempt < WRITE_RETRIES:
            log.info('Retrying in %ds...', WRITE_RETRY_DELAY)
            time.sleep(WRITE_RETRY_DELAY)
    log.error('Batch of %d points failed after %d attempts — skipping', len(batch), WRITE_RETRIES)
    return False


def run_import(prefix, range_start, range_end, dry_run, batch_size, write_pause, write_api, influx_org, influx_bucket):
    from influxdb_client import Point

    measurements = get_measurements()
    log.info('Found %d measurements to import', len(measurements))
    log.info('Batch size: %d  Write pause: %.2fs  Page size: %d', batch_size, write_pause, V1_PAGE_SIZE)

    total = skipped = written = failed_points = preview = 0
    batch = []

    for measurement in measurements:
        mapping = MEASUREMENT_MAP.get(measurement)
        if not mapping:
            log.warning('No topic mapping for "%s" — skipping', measurement)
            skipped += 1
            continue

        short_topic, preferred_field = mapping
        field = pick_field(get_field_keys(measurement), preferred_field)
        if not field:
            log.warning('No fields found for "%s" — skipping', measurement)
            skipped += 1
            continue

        log.info('Importing: %-35s field=%-12s → %s', measurement, field, short_topic)

        for ts_ns, float_val in stream_v1_measurement(measurement, field, range_start, range_end):
            total += 1
            ts_utc = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)

            if dry_run:
                if preview < 40:
                    log.info('[DRY-RUN] topic=%-55s value=%-12.4f  time=%s',
                             short_topic, float_val, ts_utc.isoformat())
                    preview += 1
                elif preview == 40:
                    log.info('[DRY-RUN] (further previews suppressed)')
                    preview += 1
                continue

            batch.append(
                Point('solar')
                .tag('topic', short_topic)
                .tag('prefix', prefix)
                .field('value', float_val)
                .time(ts_utc)
            )

            if len(batch) >= batch_size:
                if write_batch_with_retry(write_api, influx_bucket, influx_org, batch):
                    written += len(batch)
                else:
                    failed_points += len(batch)
                batch = []
                if written % 10000 == 0 and written > 0:
                    log.info('  Written %d points so far...', written)
                if write_pause > 0:
                    time.sleep(write_pause)

    if batch and not dry_run:
        if write_batch_with_retry(write_api, influx_bucket, influx_org, batch):
            written += len(batch)
        else:
            failed_points += len(batch)

    if failed_points:
        log.warning('Failed to write %d points after retries', failed_points)

    return total, skipped, written


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Import Solar Assistant backup into InfluxDB 2.x')
    parser.add_argument('backup')
    parser.add_argument('--dry-run',               action='store_true')
    parser.add_argument('--inspect',               action='store_true')
    parser.add_argument('--range-start')
    parser.add_argument('--range-end')
    parser.add_argument('--prefix',                default='solar_assistant')
    parser.add_argument('--batch-size',            type=int,   default=200)
    parser.add_argument('--write-pause',           type=float, default=0.0,
                        help='Seconds to pause between batches (default: 0.0)')
    parser.add_argument('--docker-network',        default=DEFAULT_DOCKER_NETWORK)
    parser.add_argument('--staging-container-dir', default=DEFAULT_STAGING_CONTAINER_DIR)
    parser.add_argument('--keep-container',        action='store_true')
    args = parser.parse_args()

    range_start = datetime.fromisoformat(args.range_start).replace(tzinfo=timezone.utc) if args.range_start else None
    range_end   = datetime.fromisoformat(args.range_end).replace(tzinfo=timezone.utc)   if args.range_end   else None

    staging_host_dir = get_staging_host_dir(args.staging_container_dir)

    write_api = None
    if not args.dry_run and not args.inspect:
        if not INFLUX_URL or not INFLUX_TOKEN:
            log.error('INFLUX_URL and INFLUX_TOKEN must be set')
            sys.exit(1)
        try:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS
            client = InfluxDBClient(
                url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG,
                timeout=WRITE_TIMEOUT_SECS * 1000,
            )
            write_api = client.write_api(write_options=SYNCHRONOUS)
            log.info('Connected to InfluxDB 2.x: %s  bucket=%s  org=%s', INFLUX_URL, INFLUX_BUCKET, INFLUX_ORG)
        except Exception as e:
            log.error('Failed to connect to InfluxDB 2.x: %s', e)
            sys.exit(1)
    elif args.dry_run:
        log.info('*** DRY-RUN MODE ***')

    container_id = None
    try:
        extract_backup_to_staging(args.backup, args.staging_container_dir)
        container_id = start_v1_container(staging_host_dir, args.docker_network)
        wait_for_v1()
        run_restore()
        check_container_logs(10)

        if args.inspect:
            inspect_measurements()
            return

        total, skipped, written = run_import(
            args.prefix, range_start, range_end,
            args.dry_run, args.batch_size, args.write_pause,
            write_api, INFLUX_ORG, INFLUX_BUCKET
        )

        log.info('--- Import complete ---')
        if args.dry_run:
            log.info('Would have written %d points (%d skipped)', total, skipped)
        else:
            log.info('Written  : %d points', written)
            log.info('Skipped  : %d unmapped measurements', skipped)

    finally:
        if container_id and not args.keep_container:
            stop_v1_container()
        try:
            if os.path.exists(args.staging_container_dir):
                shutil.rmtree(args.staging_container_dir)
                log.info('Staging dir cleaned up.')
        except Exception:
            pass


if __name__ == '__main__':
    main()
