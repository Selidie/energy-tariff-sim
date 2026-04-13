"""api.py — Flask REST API for the energy tariff simulator."""
import os
import json
import uuid
import shutil
import zipfile
import tempfile
import threading
import subprocess
import logging
import queue
import time
import re
import yaml
import requests
from collections import deque
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_file, Response, stream_with_context
from flask_cors import CORS
from app import config as cfg_module
from app.ingest import run_ingest, load_raw, check_bridge
from app.aggregate import run_aggregate, load_aggregated
from app.tariffs import load_tariffs
from app.simulate import (compare_tariffs, simulate_tariff,
                           daily_summary, monthly_summary, yearly_summary,
                           _serialise_period)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

_UI_PATH       = os.path.join(os.path.dirname(__file__), 'ui.html')
_CONFIG_PATH   = os.path.join(os.path.dirname(__file__), 'config.html')
_SETTINGS_PATH = os.environ.get(
    'CONFIG_PATH',
    os.path.join(os.path.dirname(__file__), '..', 'config', 'settings.yaml')
)

APP_VERSION = os.environ.get('APP_VERSION', 'dev')

try:
    _cfg     = cfg_module.load(_SETTINGS_PATH)
    _tariffs = load_tariffs(_cfg)
    log.info('Loaded %d tariff(s) from %s', len(_tariffs), _SETTINGS_PATH)
except Exception as _boot_err:
    log.error('FATAL: Could not load config at startup: %s', _boot_err)
    raise


def _tz()             -> str: return _cfg.get('simulation', {}).get('timezone', 'UTC')
def _baseline_id()    -> str: return _cfg.get('simulation', {}).get('baseline_tariff_id', '')
def _history_range()  -> str: return _cfg.get('simulation', {}).get('history_range', '700d')
def _results_path()   -> str: return _cfg.get('storage', {}).get('results_path', '/app/data/results.json')

def _ordered_tariffs():
    bid = _baseline_id()
    if not bid:
        return _tariffs
    return [t for t in _tariffs if t.id == bid] + [t for t in _tariffs if t.id != bid]


# ── Results persistence ────────────────────────────────────────────────────

def _save_results(data: dict):
    path = _results_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data['saved_at'] = datetime.now(timezone.utc).isoformat()
        with open(path, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        log.warning('Could not save results: %s', e)

def _load_results():
    try:
        with open(_results_path(), 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning('Could not load results: %s', e)
        return None

def _clear_results():
    try:
        path = _results_path()
        if os.path.exists(path):
            os.remove(path)
    except Exception as e:
        log.warning('Could not clear results: %s', e)


# ── Error handlers ─────────────────────────────────────────────────────────

@app.errorhandler(Exception)
def handle_exception(e):
    log.exception('Unhandled exception in %s %s', request.method, request.path)
    return jsonify({'success': False, 'error': f'Internal server error: {str(e)}'}), 500

@app.errorhandler(404)
def handle_404(e):
    if request.path.startswith('/api/') or request.path in (
            '/ingest', '/aggregate', '/run', '/simulate', '/compare', '/health', '/tariffs'):
        return jsonify({'success': False, 'error': f'Not found: {request.path}'}), 404
    return jsonify({'success': False, 'error': 'Not found'}), 404


# ── Pages ──────────────────────────────────────────────────────────────────

@app.get('/')
def ui():
    return send_file(_UI_PATH)

@app.get('/config')
def config_page():
    return send_file(_CONFIG_PATH)


# ── Health ─────────────────────────────────────────────────────────────────

@app.get('/health')
def health():
    try:
        bridge = check_bridge(_cfg['mqtt']['api_url'])
    except Exception as e:
        bridge = {'ok': False, 'reason': str(e)}
    return jsonify({
        'status':             'ok',
        'version':            APP_VERSION,
        'tariffs':            [t.id for t in _tariffs],
        'baseline_tariff_id': _baseline_id(),
        'timezone':           _tz(),
        'history_range':      _history_range(),
        'bridge':             bridge,
        'has_results':        _load_results() is not None,
    })


# ── Config API ─────────────────────────────────────────────────────────────

@app.get('/api/config')
def get_config():
    try:
        with open(_SETTINGS_PATH, 'r') as f:
            raw = yaml.safe_load(f)
        return jsonify({'success': True, 'config': raw})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.post('/api/config')
def save_config():
    global _cfg, _tariffs
    body = request.get_json(force=True)
    if not body:
        return jsonify({'success': False, 'error': 'No JSON body'}), 400
    try:
        with open(_SETTINGS_PATH, 'r') as f:
            current = yaml.safe_load(f) or {}

        mqtt_host = body.get('mqtt_host', '').strip()
        if mqtt_host:
            current.setdefault('mqtt', {})['api_url'] = f"http://{mqtt_host}:{body.get('mqtt_port','5003')}"

        tz = body.get('timezone', '').strip()
        if tz:
            current.setdefault('simulation', {})['timezone'] = tz

        baseline_id = body.get('baseline_tariff_id', '').strip()
        if baseline_id:
            current.setdefault('simulation', {})['baseline_tariff_id'] = baseline_id

        tariffs_in = body.get('tariffs', [])
        built = []
        for t in tariffs_in:
            ttype = t.get('type')
            tid   = t.get('id') or _slugify(t.get('name', 'tariff'))
            entry = {
                'id':              tid,
                'name':            t.get('name', 'Unnamed'),
                'type':            'flat' if ttype == 'flat' else 'day_night',
                'standing_charge': float(t.get('standing_charge', 0)),
                'export_rate':     float(t.get('export_rate', 0)),
            }
            if ttype == 'flat':
                entry['import_rate'] = float(t.get('import_rate', 0))
            else:
                ns = t.get('night_start', '00:00')
                ne = t.get('night_end',   '07:00')
                entry['day']   = {'rate': float(t.get('day_rate', 0)),   'start': ne,  'end': '00:00'}
                entry['night'] = {'rate': float(t.get('night_rate', 0)), 'start': ns,  'end': ne}
            built.append(entry)

        if built:
            current['tariffs'] = built

        with open(_SETTINGS_PATH, 'w') as f:
            yaml.dump(current, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        _cfg     = cfg_module.load(_SETTINGS_PATH)
        _tariffs = load_tariffs(_cfg)
        return jsonify({'success': True})
    except Exception as e:
        log.exception("Failed to save config")
        return jsonify({'success': False, 'error': str(e)}), 500


def _slugify(name: str) -> str:
    slug = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
    return slug or str(uuid.uuid4())[:8]


# ── Results API ────────────────────────────────────────────────────────────

@app.get('/api/results')
def get_results():
    data = _load_results()
    return jsonify({'success': True, 'results': data})

@app.delete('/api/results')
def delete_results():
    _clear_results()
    return jsonify({'success': True})


# ── Bridge passthrough ─────────────────────────────────────────────────────

@app.get('/api/bridge/topics')
def bridge_topics():
    try:
        api_url = _cfg['mqtt']['api_url'].rstrip('/')
        data = requests.get(f'{api_url}/topics', timeout=10).json()
        return jsonify({
            'success': True, 'bridge_url': api_url,
            'configured_topics': _cfg['mqtt'].get('topics', {}),
            'topic_count': data.get('topic_count', 0),
            'mqtt_connected': data.get('mqtt_connected', False),
            'topics': data.get('topics', {}),
        })
    except requests.exceptions.ConnectionError as e:
        return jsonify({'success': False, 'error': f'Cannot reach bridge: {e}'}), 502
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/api/bridge/topics/numeric')
def bridge_topics_numeric():
    try:
        api_url = _cfg['mqtt']['api_url'].rstrip('/')
        data = requests.get(f'{api_url}/topics/numeric', timeout=10).json()

        return jsonify({'success': True, 'topics': data.get('topics', []),
                        'configured_topics': _cfg['mqtt'].get('topics', {})})
    except requests.exceptions.ConnectionError as e:
        return jsonify({'success': False, 'error': f'Cannot reach bridge: {e}'}), 502
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/api/bridge/diagnose')
def bridge_diagnose():
    try:
        api_url    = _cfg['mqtt']['api_url'].rstrip('/')
        health     = requests.get(f'{api_url}/health', timeout=10).json()
        all_topics = requests.get(f'{api_url}/topics', timeout=10).json().get('topics', {})
        prefix     = health.get('prefix', 'solar_assistant')
        configured = _cfg['mqtt'].get('topics', {})
        topic_status = {}
        for label, short_topic in configured.items():
            full_topic = f'{prefix}/{short_topic}'
            in_bridge  = short_topic in all_topics or full_topic in all_topics
            live_data  = all_topics.get(full_topic) or all_topics.get(short_topic)
            topic_status[label] = {
                'configured_as': short_topic, 'full_topic': full_topic, 'found': in_bridge,
                'latest_value': live_data.get('value') if live_data else None,
                'latest_ts':    live_data.get('ts')    if live_data else None,
            }
        influx_ok = health.get('influx_enabled', False)
        return jsonify({
            'success': True, 'bridge_url': api_url,
            'mqtt_connected': health.get('mqtt_connected', False),
            'influx_enabled': influx_ok, 'broker': health.get('broker'),
            'prefix': prefix, 'topic_count': health.get('topic_count', 0),
            'configured_topics': topic_status,
            'ingest_ready': health.get('mqtt_connected') and influx_ok
                            and all(v['found'] for v in topic_status.values()),
        })
    except requests.exceptions.ConnectionError as e:
        return jsonify({'success': False, 'error': f'Cannot reach bridge: {e}'}), 502
    except Exception as e:
        log.exception('Diagnose failed')
        return jsonify({'success': False, 'error': str(e)}), 500


# ── Pipeline ───────────────────────────────────────────────────────────────

def _extract_date_range(body: dict) -> tuple:
    """Pull date_from / date_to from a JSON request body, returning (str|None, str|None)."""
    return body.get('date_from') or None, body.get('date_to') or None


@app.post('/ingest')
def ingest():
    global _cfg
    try:
        _cfg = cfg_module.load(_SETTINGS_PATH)
        body = request.get_json(force=True, silent=True) or {}
        date_from, date_to = _extract_date_range(body)
        raw_df, diag = run_ingest(_cfg, date_from=date_from, date_to=date_to)
        if raw_df.empty:
            return jsonify({'success': False, 'error': diag.get('reason', 'No data returned'), 'diag': diag}), 502
        return jsonify({'success': True, 'rows': diag.get('rows', len(raw_df)),
                        'from': diag.get('date_from'), 'to': diag.get('date_to'),
                        'topics_found': diag.get('topics_found', [])})
    except Exception as e:
        log.exception('Ingest failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.post('/aggregate')
def aggregate():
    try:
        agg_df = run_aggregate(_cfg)
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No raw data — run ingest first'}), 400
        return jsonify({'success': True, 'intervals': len(agg_df),
                        'from': str(agg_df.index.min()), 'to': str(agg_df.index.max())})
    except Exception as e:
        log.exception('Aggregate failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.post('/run')
def run_all():
    global _cfg, _tariffs
    try:
        _cfg     = cfg_module.load(_SETTINGS_PATH)
        _tariffs = load_tariffs(_cfg)
        body = request.get_json(force=True, silent=True) or {}
        date_from, date_to = _extract_date_range(body)
        raw_df, diag = run_ingest(_cfg, date_from=date_from, date_to=date_to)
        if raw_df.empty:
            return jsonify({'success': False, 'error': diag.get('reason', 'Ingest returned no data'), 'diag': diag}), 502
        agg_df = run_aggregate(_cfg, raw_df)
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'Aggregation produced no intervals'}), 500
        result = compare_tariffs(agg_df, _ordered_tariffs(), tz=_tz())
        result['baseline_id'] = _baseline_id() or result.get('baseline_id')
        _save_results(result)
        return jsonify({'success': True, **result})
    except Exception as e:
        log.exception('Run all failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/simulate')
def simulate():
    try:
        agg_df = load_aggregated(_cfg['storage']['aggregated_path'])
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No aggregated data — run ingest + aggregate first'}), 400
        result = compare_tariffs(agg_df, _ordered_tariffs(), tz=_tz())
        result['baseline_id'] = _baseline_id() or result.get('baseline_id')
        _save_results(result)
        return jsonify({'success': True, **result})
    except Exception as e:
        log.exception('Simulate failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/compare')
def compare():
    try:
        agg_df = load_aggregated(_cfg['storage']['aggregated_path'])
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No aggregated data'}), 400
        result = compare_tariffs(agg_df, _ordered_tariffs(), tz=_tz())
        result['baseline_id'] = _baseline_id() or result.get('baseline_id')
        return jsonify({'success': True, 'comparison': result['comparison'],
                        'baseline_id': result['baseline_id']})
    except Exception as e:
        log.exception('Compare failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/results/daily')
def results_daily():
    try:
        tariff_id = request.args.get('tariff')
        tariff    = _get_tariff(tariff_id)
        if not tariff:
            return jsonify({'success': False, 'error': f'Unknown tariff: {tariff_id}'}), 404
        agg_df = load_aggregated(_cfg['storage']['aggregated_path'])
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No aggregated data'}), 400
        detail = simulate_tariff(agg_df, tariff, tz=_tz())
        daily  = daily_summary(detail, tz=_tz()).reset_index().to_dict(orient='records')
        return jsonify({'success': True, 'tariff_id': tariff.id, 'daily': daily})
    except Exception as e:
        log.exception('Results daily failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/results/monthly')
def results_monthly():
    try:
        tariff_id = request.args.get('tariff')
        tariff    = _get_tariff(tariff_id)
        if not tariff:
            return jsonify({'success': False, 'error': f'Unknown tariff: {tariff_id}'}), 404
        agg_df = load_aggregated(_cfg['storage']['aggregated_path'])
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No aggregated data'}), 400
        detail  = simulate_tariff(agg_df, tariff, tz=_tz())
        daily   = daily_summary(detail, tz=_tz())
        monthly = monthly_summary(daily)
        return jsonify({'success': True, 'tariff_id': tariff.id,
                        'monthly': _serialise_period(monthly, 'month')})
    except Exception as e:
        log.exception('Results monthly failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/results/yearly')
def results_yearly():
    try:
        tariff_id = request.args.get('tariff')
        tariff    = _get_tariff(tariff_id)
        if not tariff:
            return jsonify({'success': False, 'error': f'Unknown tariff: {tariff_id}'}), 404
        agg_df = load_aggregated(_cfg['storage']['aggregated_path'])
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No aggregated data'}), 400
        detail = simulate_tariff(agg_df, tariff, tz=_tz())
        daily  = daily_summary(detail, tz=_tz())
        yearly = yearly_summary(daily)
        return jsonify({'success': True, 'tariff_id': tariff.id,
                        'yearly': _serialise_period(yearly, 'year')})
    except Exception as e:
        log.exception('Results yearly failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/tariffs')
def list_tariffs():
    return jsonify({'success': True, 'tariffs': [t.to_dict() for t in _tariffs]})


def _get_tariff(tariff_id):
    if not tariff_id:
        return _tariffs[0] if _tariffs else None
    return next((t for t in _tariffs if t.id == tariff_id), None)


# ── Solar Assistant Import API ─────────────────────────────────────────────

_sa_import_lock    = threading.Lock()
_sa_import_running = False

_sa_import_state = {
    'written':    0,
    'skipped':    0,
    'last_line':  '',
    'done':       False,
    'failed':     False,
    'err_msg':    '',
    'started_at': None,
}

_sa_import_log_buffer: deque = deque(maxlen=200)
_sa_import_log_lock = threading.Lock()

_SA_UPLOAD_DIR = '/tmp/sa_webui_upload'
_SSE_KEEPALIVE_INTERVAL = 15


def _sa_upload_path() -> str:
    if not os.path.isdir(_SA_UPLOAD_DIR):
        return None
    zips = [f for f in os.listdir(_SA_UPLOAD_DIR) if f.endswith('.zip')]
    return os.path.join(_SA_UPLOAD_DIR, zips[0]) if zips else None


def _sa_probe_zip_dates(zip_path: str) -> dict:
    """
    Read the .manifest file(s) inside the zip and extract the earliest/latest
    timestamps without spinning up Docker.

    Only timestamps within a plausible range are accepted — Solar Assistant
    didn't exist before 2018, and future dates are impossible.  This prevents
    InfluxDB shard boundary artefacts (which can include epoch-zero or other
    implausible dates) from polluting the date picker range shown to the user.
    """
    earliest = None
    latest   = None
    file_count = 0
    total_bytes = 0

    # Solar Assistant was first released in 2018; nothing earlier is real data.
    _MIN_PLAUSIBLE = datetime(2018, 1, 1, tzinfo=timezone.utc)
    _MAX_PLAUSIBLE = datetime.now(timezone.utc)

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            members = zf.namelist()
            file_count = len(members)
            for name in members:
                info = zf.getinfo(name)
                total_bytes += info.file_size

            manifest_files = [m for m in members if m.endswith('.manifest')]
            for mf in manifest_files:
                try:
                    content = zf.read(mf).decode('utf-8', errors='replace')
                    for ts in re.findall(r'"(?:start|end)"\s*:\s*"([^"]+)"', content):
                        try:
                            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                            # Reject anything outside the plausible window
                            if dt < _MIN_PLAUSIBLE or dt > _MAX_PLAUSIBLE:
                                continue
                            if earliest is None or dt < earliest:
                                earliest = dt
                            if latest is None or dt > latest:
                                latest = dt
                        except ValueError:
                            pass
                except Exception:
                    pass
    except Exception as e:
        return {'success': False, 'error': str(e)}

    return {
        'success':     True,
        'file_count':  file_count,
        'total_bytes': total_bytes,
        'date_start':  earliest.strftime('%Y-%m-%d') if earliest else None,
        'date_end':    latest.strftime('%Y-%m-%d')   if latest   else None,
    }


def _sa_run_background(cmd: list, env: dict):
    global _sa_import_running, _sa_import_state

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )

        for line in iter(proc.stdout.readline, ''):
            line = line.rstrip('\n')
            if not line:
                continue

            m = re.search(r'Written\s+(\d+)\s+points', line)
            if m:
                _sa_import_state['written'] = int(m.group(1))
            m = re.search(r'Skipped\s+:\s+(\d+)', line)
            if m:
                _sa_import_state['skipped'] = int(m.group(1))

            _sa_import_state['last_line'] = line

            entry = {'line': line, 'written': _sa_import_state['written']}
            with _sa_import_log_lock:
                _sa_import_log_buffer.append(entry)

        proc.stdout.close()
        rc = proc.wait()

        if rc != 0:
            _sa_import_state['failed']  = True
            _sa_import_state['err_msg'] = f'Process exited with code {rc}'

    except Exception as e:
        _sa_import_state['failed']  = True
        _sa_import_state['err_msg'] = str(e)
        log.exception('SA import subprocess error')

    finally:
        _sa_import_state['done'] = True
        _sa_import_running = False
        _sa_import_lock.release()


@app.post('/api/sa-import/upload')
def sa_import_upload():
    global _sa_import_running
    if _sa_import_running:
        return jsonify({'success': False, 'error': 'An import is already running'}), 409

    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400

    f = request.files['file']
    if not f.filename or not f.filename.lower().endswith('.zip'):
        return jsonify({'success': False, 'error': 'File must be a .zip'}), 400

    if os.path.isdir(_SA_UPLOAD_DIR):
        shutil.rmtree(_SA_UPLOAD_DIR)
    os.makedirs(_SA_UPLOAD_DIR, exist_ok=True)

    dest = os.path.join(_SA_UPLOAD_DIR, 'backup.zip')
    try:
        f.save(dest)
    except Exception as e:
        return jsonify({'success': False, 'error': f'Failed to save file: {e}'}), 500

    probe = _sa_probe_zip_dates(dest)
    probe['filename'] = f.filename
    return jsonify(probe)


@app.post('/api/sa-import/clear')
def sa_import_clear():
    if os.path.isdir(_SA_UPLOAD_DIR):
        shutil.rmtree(_SA_UPLOAD_DIR)
    return jsonify({'success': True})


@app.get('/api/sa-import/stream')
def sa_import_stream():
    global _sa_import_running, _sa_import_state

    zip_path = _sa_upload_path()
    if not zip_path and not _sa_import_running:
        def err():
            yield 'event: error\ndata: {"error": "No backup file uploaded"}\n\n'
        return Response(stream_with_context(err()), mimetype='text/event-stream')

    range_start = request.args.get('range_start', '').strip()

    if not _sa_import_running:
        if not _sa_import_lock.acquire(blocking=False):
            def busy():
                yield 'event: error\ndata: {"error": "Import already running"}\n\n'
            return Response(stream_with_context(busy()), mimetype='text/event-stream')

        _sa_import_running = True

        _sa_import_state.update({
            'written':    0,
            'skipped':    0,
            'last_line':  '',
            'done':       False,
            'failed':     False,
            'err_msg':    '',
            'started_at': datetime.now(timezone.utc).isoformat(),
        })
        with _sa_import_log_lock:
            _sa_import_log_buffer.clear()

        script = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', 'scripts', 'sa_import.py')
        )
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'

        cmd = ['python3', script, zip_path]
        if range_start:
            cmd += ['--range-start', range_start + 'T00:00:00']

        log.info('SA import starting: %s', ' '.join(cmd))

        t = threading.Thread(target=_sa_run_background, args=(cmd, env), daemon=True)
        t.start()

    def generate():
        with _sa_import_log_lock:
            buffered = list(_sa_import_log_buffer)

        for entry in buffered:
            payload = json.dumps(entry)
            yield f'data: {payload}\n\n'

        sent = len(buffered)
        last_keepalive = time.monotonic()

        while True:
            with _sa_import_log_lock:
                current = list(_sa_import_log_buffer)

            for entry in current[sent:]:
                payload = json.dumps(entry)
                yield f'data: {payload}\n\n'
                last_keepalive = time.monotonic()
            sent = len(current)

            if _sa_import_state['done']:
                if _sa_import_state['failed']:
                    yield f'event: error\ndata: {json.dumps({"error": _sa_import_state["err_msg"]})}\n\n'
                else:
                    summary = json.dumps({
                        "written": _sa_import_state["written"],
                        "skipped": _sa_import_state["skipped"],
                    })
                    yield f'event: done\ndata: {summary}\n\n'
                break

            if time.monotonic() - last_keepalive >= _SSE_KEEPALIVE_INTERVAL:
                yield ': keepalive\n\n'
                last_keepalive = time.monotonic()

            time.sleep(1)

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control':    'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )


@app.get('/api/sa-import/status')
def sa_import_status():
    return jsonify({
        'running':     _sa_import_running,
        'has_upload':  _sa_upload_path() is not None,
        'written':     _sa_import_state['written'],
        'skipped':     _sa_import_state['skipped'],
        'last_line':   _sa_import_state['last_line'],
        'done':        _sa_import_state['done'],
        'failed':      _sa_import_state['failed'],
        'err_msg':     _sa_import_state['err_msg'],
        'started_at':  _sa_import_state['started_at'],
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5011)), debug=False)
