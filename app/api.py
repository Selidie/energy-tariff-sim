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
import pandas as pd
from collections import deque
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_file, Response, stream_with_context
from flask_cors import CORS
from app import config as cfg_module
from app.ingest import run_ingest, run_octopus_ingest, has_octopus_account, load_raw, check_bridge
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

_OCTO_CACHE_PATH = os.environ.get(
    'OCTO_CACHE_PATH',
    os.path.join(os.path.dirname(__file__), '..', 'data', 'octopus_tariffs.json')
)

from app import edf_client as _edf
from app.tariffs import EdfFlatTariff, EdfDayNightTariff, EdfTimeOfUseTariff

_EDF_CACHE_PATH = os.environ.get(
    'EDF_CACHE_PATH',
    os.path.join(os.path.dirname(__file__), '..', 'data', 'edf_tariffs.json')
)


def _save_octo_tariffs():
    """Persist all in-memory Octopus tariffs to disk as JSON."""
    from app.tariffs import OctopusFlatTariff, OctopusDayNightTariff, OctopusTimeOfUseTariff, OctopusSEGTariff
    octo = [t.to_dict() for t in _tariffs
            if isinstance(t, (OctopusFlatTariff, OctopusDayNightTariff, OctopusTimeOfUseTariff, OctopusSEGTariff))]
    try:
        cache_dir = os.path.dirname(_OCTO_CACHE_PATH)
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        with open(_OCTO_CACHE_PATH, 'w') as f:
            json.dump(octo, f)
        log.info('Saved %d Octopus tariff(s) to %s', len(octo), _OCTO_CACHE_PATH)
    except Exception as e:
        log.warning('Could not save Octopus tariffs: %s', e)


def _load_octo_tariffs() -> list:
    """Reload persisted Octopus tariffs from disk and return as tariff objects."""
    try:
        with open(_OCTO_CACHE_PATH, 'r') as f:
            entries = json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        log.warning('Could not load Octopus tariffs: %s', e)
        return []

    loaded = []
    for entry in entries:
        try:
            ttype = entry.get('type')
            if ttype == 'octopus_flat':
                cfg = {
                    'id':             entry['id'],
                    'name':           entry['name'],
                    'standing_charge': entry.get('standing_charge', 0),
                    'export_rate':    entry.get('export_rate', 0),
                    'import_rate':    entry.get('import_rate', 0),
                    'product_code':   entry.get('product_code', ''),
                    'tariff_code':    entry.get('tariff_code', ''),
                    'gsp_region':     entry.get('gsp_region', ''),
                }
                loaded.append(OctopusFlatTariff(cfg))
            elif ttype == 'octopus_day_night':
                from app.tariffs import OctopusDayNightTariff
                cfg = {
                    'id':             entry['id'],
                    'name':           entry['name'],
                    'standing_charge': entry.get('standing_charge', 0),
                    'export_rate':    entry.get('export_rate', 0),
                    'day_rate':       entry.get('day_rate', 0),
                    'night_rate':     entry.get('night_rate', 0),
                    'night_start':    entry.get('night_start', '00:00'),
                    'night_end':      entry.get('night_end',   '07:00'),
                    'product_code':   entry.get('product_code', ''),
                    'tariff_code':    entry.get('tariff_code', ''),
                    'gsp_region':     entry.get('gsp_region', ''),
                }
                loaded.append(OctopusDayNightTariff(cfg))
            elif ttype == 'octopus_seg':
                from app.tariffs import OctopusSEGTariff
                cfg = {
                    'id':                entry['id'],
                    'name':              entry['name'],
                    'standing_charge':   entry.get('standing_charge', 0),
                    'export_rate':       entry.get('export_rate', 0),
                    'product_code':      entry.get('product_code', ''),
                    'tariff_code':       entry.get('tariff_code', ''),
                    'gsp_region':        entry.get('gsp_region', ''),
                    'is_current_export': entry.get('is_current_export', False),
                }
                loaded.append(OctopusSEGTariff(cfg))
            elif ttype == 'octopus_agile':
                # For time-of-use tariffs we need the raw rates — stored in
                # the octopus_client cache, so re-fetch from there
                from app import octopus_client as _octo
                from app.ingest import has_octopus_account, run_octopus_ingest
                from datetime import datetime as _dt, timezone as _tz_mod
                product_code = entry.get('product_code', '')
                tariff_code  = entry.get('tariff_code', '')
                gsp_region   = entry.get('gsp_region', '')
                today        = _dt.now(_tz_mod.utc)
                days         = _parse_history_days(_history_range())
                date_from    = _dt(today.year - (days // 365),
                                   today.month, today.day, tzinfo=_tz_mod.utc)
                rates = _octo.get_tariff_unit_rates(
                    product_code, tariff_code, date_from, today)
                cfg = {
                    'id':             entry['id'],
                    'name':           entry['name'],
                    'standing_charge': entry.get('standing_charge', 0),
                    'export_rate':    entry.get('export_rate', 0),
                    'rates':          rates,
                    'product_code':   product_code,
                    'tariff_code':    tariff_code,
                    'gsp_region':     gsp_region,
                }
                loaded.append(OctopusTimeOfUseTariff(cfg))
        except Exception as e:
            log.warning('Could not restore Octopus tariff %s: %s', entry.get('id'), e)

    log.info('Restored %d Octopus tariff(s) from %s', len(loaded), _OCTO_CACHE_PATH)
    return loaded


def _save_edf_tariffs():
    """Persist all in-memory EDF tariffs to disk as JSON."""
    edf = [t.to_dict() for t in _tariffs
           if isinstance(t, (EdfFlatTariff, EdfDayNightTariff, EdfTimeOfUseTariff))]
    try:
        cache_dir = os.path.dirname(_EDF_CACHE_PATH)
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        with open(_EDF_CACHE_PATH, 'w') as f:
            json.dump(edf, f)
        log.info('Saved %d EDF tariff(s) to %s', len(edf), _EDF_CACHE_PATH)
    except Exception as e:
        log.warning('Could not save EDF tariffs: %s', e)


def _load_edf_tariffs() -> list:
    """Reload persisted EDF tariffs from disk and return as tariff objects."""
    try:
        with open(_EDF_CACHE_PATH, 'r') as f:
            entries = json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        log.warning('Could not load EDF tariffs: %s', e)
        return []

    loaded = []
    for entry in entries:
        try:
            ttype = entry.get('type')
            base  = {
                'id':              entry['id'],
                'name':            entry['name'],
                'standing_charge': entry.get('standing_charge', 0),
                'export_rate':     entry.get('export_rate', 0),
                'product_code':    entry.get('product_code', ''),
                'tariff_code':     entry.get('tariff_code', ''),
                'gsp_region':      entry.get('gsp_region', ''),
            }
            if ttype == 'edf_flat':
                loaded.append(EdfFlatTariff({**base, 'import_rate': entry.get('import_rate', 0)}))
            elif ttype == 'edf_day_night':
                loaded.append(EdfDayNightTariff({
                    **base,
                    'day_rate':    entry.get('day_rate', 0),
                    'night_rate':  entry.get('night_rate', 0),
                    'night_start': entry.get('night_start', '00:00'),
                    'night_end':   entry.get('night_end',   '07:00'),
                }))
            elif ttype == 'edf_agile':
                today     = _dt.now(_tz_mod.utc)
                days      = _parse_history_days(_history_range())
                date_from = _dt(today.year - (days // 365), today.month, today.day, tzinfo=_tz_mod.utc)
                rates     = _edf.get_tariff_unit_rates(
                    entry.get('product_code', ''),
                    entry.get('tariff_code', ''),
                    date_from, today,
                )
                loaded.append(EdfTimeOfUseTariff({**base, 'rates': rates}))
        except Exception as e:
            log.warning('Could not restore EDF tariff %s: %s', entry.get('id'), e)

    log.info('Restored %d EDF tariff(s) from %s', len(loaded), _EDF_CACHE_PATH)
    return loaded


def _reload_tariffs():
    global _cfg, _tariffs
    _cfg     = cfg_module.load(_SETTINGS_PATH)
    _tariffs = load_tariffs(_cfg)
    _tariffs += _load_octo_tariffs()
    _tariffs += _load_edf_tariffs()

try:
    _cfg     = cfg_module.load(_SETTINGS_PATH)
    _tariffs = load_tariffs(_cfg)
    _tariffs += _load_octo_tariffs()
    _tariffs += _load_edf_tariffs()
    log.info('Loaded %d tariff(s) from %s', len(_tariffs), _SETTINGS_PATH)
except Exception as _boot_err:
    log.error('FATAL: Could not load config at startup: %s', _boot_err)
    raise

_config_changed_at = datetime.now(timezone.utc).isoformat()

# ── Octopus month cache ────────────────────────────────────────────────────
# Keyed by "YYYY-MM-import" / "YYYY-MM-export".
# Each entry: { 'slots': [...], 'fetched_at': datetime }
# Entries older than _OCTO_CACHE_TTL_HOURS are re-fetched automatically.
_octo_month_cache: dict = {}
_OCTO_CACHE_TTL_HOURS   = 6

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

def _json_default(obj):
    import datetime as _dt_module
    if isinstance(obj, (_dt_module.date, _dt_module.datetime)):
        return obj.isoformat()
    raise TypeError(f'Object of type {type(obj)} is not JSON serializable')

def _save_results(data: dict):
    path = _results_path()
    try:
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        data['saved_at'] = datetime.now(timezone.utc).isoformat()
        with open(path, 'w') as f:
            json.dump(data, f, default=_json_default)
        log.info('Results saved to %s', path)
    except Exception as e:
        log.error('Could not save results to %s: %s', path, e)

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


# ── Static assets ──────────────────────────────────────────────────────────

_ICON_PATH = os.path.join(os.path.dirname(__file__), 'icon.png')

@app.get('/favicon.ico')
def favicon():
    return send_file(_ICON_PATH, mimetype='image/png')

@app.get('/static/icon.png')
def static_icon():
    return send_file(_ICON_PATH, mimetype='image/png')


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
        'config_changed_at':  _config_changed_at,
    })


# ── Config API ─────────────────────────────────────────────────────────────

@app.get('/api/config')
def get_config():
    try:
        with open(_SETTINGS_PATH, 'r') as f:
            raw = yaml.safe_load(f)

        # Append any in-memory Octopus tariffs so the UI can restore them
        # after a page navigation (they are not written to settings.yaml)
        from app.tariffs import OctopusFlatTariff, OctopusDayNightTariff, OctopusTimeOfUseTariff, OctopusSEGTariff
        octo_tariffs = [
            t.to_dict() for t in _tariffs
            if isinstance(t, (OctopusFlatTariff, OctopusDayNightTariff, OctopusTimeOfUseTariff, OctopusSEGTariff))
        ]
        if octo_tariffs:
            raw.setdefault('octopus_tariffs', octo_tariffs)

        edf_tariffs = [
            t.to_dict() for t in _tariffs
            if isinstance(t, (EdfFlatTariff, EdfDayNightTariff, EdfTimeOfUseTariff))
        ]
        if edf_tariffs:
            raw.setdefault('edf_tariffs', edf_tariffs)

        # Mask the stored API key — send a sentinel so the UI can show
        # "key saved" without echoing the real value to the browser.
        if raw.get('octopus_account', {}).get('api_key', '').strip():
            raw.setdefault('octopus_account', {})['api_key'] = '__saved__'

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

        # Persist Octopus account credentials.
        # If the incoming api_key is the masked sentinel '__saved__', leave
        # the stored key untouched — the user has not changed it.
        acct_in = body.get('octopus_account', {})
        if acct_in:
            stored_acct = current.setdefault('octopus_account', {})
            incoming_key = acct_in.get('api_key', '').strip()
            if incoming_key and incoming_key != '__saved__':
                stored_acct['api_key'] = incoming_key
            for field in ('account_number', 'import_mpan', 'import_serial', 'export_mpan', 'export_serial'):
                if field in acct_in:
                    stored_acct[field] = acct_in[field].strip()

        with open(_SETTINGS_PATH, 'w') as f:
            yaml.dump(current, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        global _config_changed_at
        _cfg     = cfg_module.load(_SETTINGS_PATH)
        _tariffs = load_tariffs(_cfg)
        _tariffs += _load_octo_tariffs()
        _tariffs += _load_edf_tariffs()
        _config_changed_at = datetime.now(timezone.utc).isoformat()
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


def _slice_agg_df(agg_df, date_from, date_to):
    """Slice a DatetimeIndex DataFrame to [date_from, date_to] inclusive."""
    if agg_df.empty:
        return agg_df
    # Normalise index to UTC to ensure consistent date comparisons
    idx = agg_df.index
    if hasattr(idx, 'tz') and idx.tz is not None:
        idx_utc = idx.tz_convert('UTC')
    else:
        idx_utc = pd.to_datetime(idx, utc=True)
    mask = pd.Series(True, index=agg_df.index)
    if date_from:
        cutoff = pd.Timestamp(date_from, tz='UTC')
        mask &= idx_utc >= cutoff
    if date_to:
        cutoff = pd.Timestamp(date_to, tz='UTC') + pd.Timedelta(days=1)
        mask &= idx_utc < cutoff
    return agg_df[mask.values]


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
        _cfg = cfg_module.load(_SETTINGS_PATH)
        log.info('Running pipeline with %d tariff(s) — %s', len(_tariffs), [t.id for t in _tariffs])
        body = request.get_json(force=True, silent=True) or {}
        date_from, date_to = _extract_date_range(body)

        agg_df  = None
        diag    = {}
        data_source = 'solar_assistant'

        if has_octopus_account(_cfg):
            log.info('Octopus account configured — attempting Octopus API ingest')
            fresh_df, diag = run_octopus_ingest(_cfg, date_from=date_from, date_to=date_to)
            if fresh_df.empty:
                log.warning(
                    'Octopus ingest returned no data (%s) — falling back to SA/InfluxDB path',
                    diag.get('reason', '?'),
                )
                data_source = 'solar_assistant_fallback'
            else:
                data_source = 'octopus_api'
                # Merge the fresh Octopus fetch into the full stored dataset,
                # then slice to the requested date range for simulation.
                stored_df = load_aggregated(_cfg['storage']['aggregated_path'])
                if not stored_df.empty:
                    agg_df = pd.concat([stored_df, fresh_df])
                    agg_df = agg_df[~agg_df.index.duplicated(keep='last')]
                    agg_df = agg_df.sort_index()
                else:
                    agg_df = fresh_df
                agg_df = _slice_agg_df(agg_df, date_from, date_to)

        if agg_df is None:
            raw_df, diag = run_ingest(_cfg, date_from=date_from, date_to=date_to)
            if raw_df.empty:
                return jsonify({'success': False, 'error': diag.get('reason', 'Ingest returned no data'), 'diag': diag}), 502
            agg_df = run_aggregate(_cfg, raw_df)
            if agg_df.empty:
                return jsonify({'success': False, 'error': 'Aggregation produced no intervals'}), 500
            agg_df = _slice_agg_df(agg_df, date_from, date_to)

        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No data in selected date range'}), 400

        result = compare_tariffs(agg_df, _ordered_tariffs(), tz=_tz())
        result['baseline_id'] = _baseline_id() or result.get('baseline_id')
        result['data_source'] = data_source
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
        date_from = request.args.get('date_from') or None
        date_to   = request.args.get('date_to')   or None
        agg_df = _slice_agg_df(agg_df, date_from, date_to)
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No data in selected date range'}), 400
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
        date_from = request.args.get('date_from') or None
        date_to   = request.args.get('date_to')   or None
        agg_df = _slice_agg_df(agg_df, date_from, date_to)
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No data in selected date range'}), 400
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
        date_from = request.args.get('date_from') or None
        date_to   = request.args.get('date_to')   or None
        agg_df = _slice_agg_df(agg_df, date_from, date_to)
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No data in selected date range'}), 400
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
        date_from = request.args.get('date_from') or None
        date_to   = request.args.get('date_to')   or None
        agg_df = _slice_agg_df(agg_df, date_from, date_to)
        if agg_df.empty:
            return jsonify({'success': False, 'error': 'No data in selected date range'}), 400
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


# ── EDF routes ─────────────────────────────────────────────────────────────
@app.get("/api/edf/products")
def edf_products():
    """List available EDF electricity products."""
    try:
        products = _edf.list_products()
        return jsonify({"success": True, "count": len(products), "products": products})
    except Exception as e:
        log.exception("edf_products failed")
        return jsonify({"success": False, "error": str(e)}), 500


@app.get("/api/edf/products/<product_code>/tariff-codes")
def edf_tariff_codes(product_code):
    """Return GSP region → tariff code map for an EDF product."""
    try:
        detail   = _edf.get_product_detail(product_code)
        regional = detail.get("single_register_electricity_tariffs", {})
        regions  = {}
        for suffix, payment_types in regional.items():
            ddc = payment_types.get("direct_debit_monthly", {})
            if "code" in ddc:
                regions[suffix] = ddc["code"]
        return jsonify({"success": True, "product_code": product_code, "regions": regions})
    except Exception as e:
        log.exception("edf_tariff_codes failed for %s", product_code)
        return jsonify({"success": False, "error": str(e)}), 500


@app.post("/api/edf/import-tariff")
def edf_import_tariff():
    """
    Fetch rates from the EDF API and add the tariff to the simulation.

    Expected JSON body:
      {
        "product_code": "...",
        "tariff_code":  "...",   // optional — resolved from region if absent
        "gsp_region":   "_C",   // optional, default "_C"
        "date_from":    "YYYY-MM-DD",  // optional
        "date_to":      "YYYY-MM-DD",  // optional
      }
    """
    global _tariffs, _config_changed_at
    try:
        body = request.get_json(force=True) or {}

        product_code = body.get("product_code", "").strip()
        if not product_code:
            return jsonify({"success": False, "error": "product_code is required"}), 400

        gsp_region  = body.get("gsp_region", "_C").strip()
        tariff_code = body.get("tariff_code", "").strip()

        if not tariff_code:
            detail      = _edf.get_product_detail(product_code)
            tariff_code = _edf.resolve_tariff_code(detail, gsp_region)
            if not tariff_code:
                return jsonify({
                    "success": False,
                    "error":   f"Could not resolve tariff code for {product_code} / {gsp_region}",
                }), 400

        today   = _dt.now(_tz_mod.utc)
        date_to = _dt.fromisoformat(body["date_to"]).replace(tzinfo=_tz_mod.utc) \
                  if body.get("date_to") else today
        if body.get("date_from"):
            date_from = _dt.fromisoformat(body["date_from"]).replace(tzinfo=_tz_mod.utc)
        else:
            days      = _parse_history_days(_history_range())
            date_from = _dt(today.year - (days // 365), today.month, today.day, tzinfo=_tz_mod.utc)

        rates    = _edf.get_tariff_unit_rates(product_code, tariff_code, date_from, date_to)
        standing = _edf.get_tariff_standing_charges(product_code, tariff_code, date_from, date_to)

        if not rates:
            return jsonify({
                "success": False,
                "error":   "No unit rates returned from EDF API for the requested period",
            }), 502

        standing_charge_p = 0.0
        if standing:
            standing.sort(key=lambda x: x.get("valid_from", ""), reverse=True)
            standing_charge_p = float(standing[0].get("value_inc_vat", 0.0))

        try:
            detail_name = _edf.get_product_detail(product_code).get("display_name", product_code)
        except Exception:
            detail_name = product_code

        tariff_id   = _slugify(f"edf_{product_code}_{gsp_region}")
        tariff_name = f"EDF {detail_name} ({_GSP_REGION_LABELS.get(gsp_region, gsp_region)})"

        # Classify using the same tariff code structure rules as Octopus
        from app.octopus_client import classify_tariff
        tc_type   = classify_tariff(tariff_code)
        day_night = None

        if tc_type == 'dual_register':
            day_r   = _edf.get_tariff_day_rates(  product_code, tariff_code, date_from, date_to)
            night_r = _edf.get_tariff_night_rates(product_code, tariff_code, date_from, date_to)
            day_night = _parse_dual_register_rates(day_r, night_r)
        elif tc_type == 'single_register':
            day_night = _parse_day_night_slots(rates)

        base_cfg = {
            "id":              tariff_id,
            "name":            tariff_name,
            "standing_charge": standing_charge_p,
            "export_rate":     0.0,
            "product_code":    product_code,
            "tariff_code":     tariff_code,
            "gsp_region":      gsp_region,
        }

        if day_night:
            new_tariff = EdfDayNightTariff({
                **base_cfg,
                "day_rate":    day_night["day_rate"],
                "night_rate":  day_night["night_rate"],
                "night_start": day_night["night_start"],
                "night_end":   day_night["night_end"],
            })
        elif tc_type == 'agile' or len(set(r.get("value_inc_vat") for r in rates)) > 1:
            new_tariff = EdfTimeOfUseTariff({**base_cfg, "rates": rates})
        else:
            unique_rates = set(r.get("value_inc_vat") for r in rates)
            new_tariff   = EdfFlatTariff({**base_cfg, "import_rate": next(iter(unique_rates), 0.0)})

        _tariffs = [t for t in _tariffs if t.id != tariff_id]
        _tariffs.append(new_tariff)

        log.info("Imported EDF tariff %s (%s) — %d rate slots, standing %.2fp/day",
                 tariff_id, tariff_code, len(rates), standing_charge_p)

        _save_edf_tariffs()
        _config_changed_at = datetime.now(timezone.utc).isoformat()

        resp = {
            "success":           True,
            "tariff_id":         tariff_id,
            "tariff_code":       tariff_code,
            "product_code":      product_code,
            "gsp_region":        gsp_region,
            "tariff_name":       new_tariff.name,
            "tariff_type":       new_tariff.to_dict()["type"],
            "rate_slots":        len(rates),
            "standing_charge_p": standing_charge_p,
            "date_from":         date_from.date().isoformat(),
            "date_to":           date_to.date().isoformat(),
        }
        if day_night:
            resp.update({
                "day_rate_p":   day_night["day_rate"],
                "night_rate_p": day_night["night_rate"],
                "night_start":  day_night["night_start"],
                "night_end":    day_night["night_end"],
            })
        return jsonify(resp)

    except requests.exceptions.ConnectionError as e:
        return jsonify({"success": False, "error": f"Could not reach EDF API: {e}"}), 502
    except requests.exceptions.HTTPError as e:
        return jsonify({"success": False, "error": f"EDF API error: {e}"}), 502
    except Exception as e:
        log.exception("edf_import_tariff failed")
        return jsonify({"success": False, "error": str(e)}), 500


@app.delete("/api/edf/tariff/<tariff_id>")
def edf_remove_tariff(tariff_id):
    """Remove a previously imported EDF tariff from the in-memory list."""
    global _tariffs, _config_changed_at
    before   = len(_tariffs)
    _tariffs = [t for t in _tariffs if t.id != tariff_id]
    _save_edf_tariffs()
    _config_changed_at = datetime.now(timezone.utc).isoformat()
    return jsonify({"success": True, "removed": before - len(_tariffs)})


@app.delete("/api/edf/cache")
def edf_clear_cache():
    """Delete all locally cached EDF API responses."""
    try:
        removed = _edf.clear_cache()
        return jsonify({"success": True, "files_removed": removed})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ── Octopus Energy API  (Phase 1 — public endpoints, no auth required) ────

from app import octopus_client as _octo
from datetime import datetime as _dt, timezone as _tz_mod


def _octopus_cfg() -> dict:
    """Return the octopus sub-section of settings, or {} if absent."""
    return _cfg.get("octopus", {})

_GSP_REGION_LABELS = {
    '_A': 'East England',
    '_B': 'East Midlands',
    '_C': 'Lincolnshire / East Midlands',
    '_D': 'London',
    '_E': 'Merseyside / North Wales',
    '_F': 'Midlands',
    '_G': 'North West',
    '_H': 'South East',
    '_J': 'South',
    '_K': 'South West',
    '_L': 'Yorkshire',
    '_M': 'North Scotland',
    '_N': 'South Scotland',
    '_P': 'North Wales / Mersey',
}

@app.get("/api/octopus/products")
def octopus_products():
    """
    List available Octopus electricity products.

    Query params:
      ?brand=OCTOPUS_ENERGY   (default; pass empty string for all brands)
    """
    try:
        brand = request.args.get("brand", "OCTOPUS_ENERGY") or None
        products = _octo.list_products(brand=brand)
        return jsonify({"success": True, "count": len(products), "products": products})
    except Exception as e:
        log.exception("octopus_products failed")
        return jsonify({"success": False, "error": str(e)}), 500


@app.get("/api/octopus/products/<product_code>")
def octopus_product_detail(product_code):
    """
    Return full detail for one product, including regional tariff codes.
    """
    try:
        detail = _octo.get_product_detail(product_code)
        return jsonify({"success": True, "product": detail})
    except Exception as e:
        log.exception("octopus_product_detail failed for %s", product_code)
        return jsonify({"success": False, "error": str(e)}), 500


@app.get("/api/octopus/products/<product_code>/tariff-codes")
def octopus_tariff_codes(product_code):
    """
    Return a map of GSP region -> tariff code for a product.
    Useful for letting the user pick their region in the UI.

    Response shape:
      { "success": true,
        "regions": { "_A": "E-1R-...-A", "_C": "E-1R-...-C", ... } }
    """
    try:
        detail  = _octo.get_product_detail(product_code)
        regional = detail.get("single_register_electricity_tariffs", {})
        regions  = {}
        for suffix, payment_types in regional.items():
            ddc = payment_types.get("direct_debit_monthly", {})
            if "code" in ddc:
                regions[suffix] = ddc["code"]
        return jsonify({"success": True, "product_code": product_code, "regions": regions})
    except Exception as e:
        log.exception("octopus_tariff_codes failed for %s", product_code)
        return jsonify({"success": False, "error": str(e)}), 500

def _parse_day_night_slots(rates: list) -> dict | None:
    """
    Parse day/night rates from standard-unit-rates slots (Go/Cosy style).

    Groups slots by local-time window, then splits into night/day using a
    rate midpoint. DST boundary fragments (short windows at midnight) are
    handled by picking the dominant (most frequent) window per rate group.
    """
    from collections import defaultdict
    import zoneinfo
    from datetime import datetime as _dt

    if not rates:
        return None

    dd_rates = [r for r in rates if r.get('payment_method', 'DIRECT_DEBIT') == 'DIRECT_DEBIT']
    if not dd_rates:
        dd_rates = rates

    try:
        local_tz = zoneinfo.ZoneInfo(_tz())
    except Exception:
        local_tz = timezone.utc

    window_current_rate = {}
    window_slots        = defaultdict(list)

    for r in dd_rates:
        vf   = r.get('valid_from', '')
        vt   = r.get('valid_to',   '')
        rate = r.get('value_inc_vat')
        if not vf or not vt or rate is None:
            continue
        try:
            local_from = _dt.fromisoformat(vf.replace('Z', '+00:00')).astimezone(local_tz)
            local_to   = _dt.fromisoformat(vt.replace('Z', '+00:00')).astimezone(local_tz)
            window     = (local_from.strftime('%H:%M'), local_to.strftime('%H:%M'))
            window_slots[window].append(float(rate))
            if window not in window_current_rate:
                window_current_rate[window] = float(rate)
        except (ValueError, KeyError):
            continue

    log.info("_parse_day_night_slots: windows=%s",
             {k: round(v, 4) for k, v in window_current_rate.items()})

    if len(window_current_rate) < 2:
        log.info("_parse_day_night_slots: rejected — fewer than 2 windows")
        return None

    all_rates = list(window_current_rate.values())
    midpoint  = (min(all_rates) + max(all_rates)) / 2
    night_wins = {w: r for w, r in window_current_rate.items() if r < midpoint}
    day_wins   = {w: r for w, r in window_current_rate.items() if r >= midpoint}

    if not night_wins or not day_wins:
        log.info("_parse_day_night_slots: rejected — could not split into 2 rate groups")
        return None

    def _dominant(wins):
        return max(wins.keys(), key=lambda w: len(window_slots[w]))

    night_window = _dominant(night_wins)
    day_window   = _dominant(day_wins)
    night_start, night_end = night_window
    night_rate = window_current_rate[night_window]
    day_rate   = window_current_rate[day_window]

    log.info("_parse_day_night_slots: night=%s@%.4f day=%s@%.4f",
             night_window, night_rate, day_window, day_rate)

    return {
        'night_rate':  round(night_rate, 4),
        'day_rate':    round(day_rate,   4),
        'night_start': night_start,
        'night_end':   night_end,
    }

def _parse_dual_register_rates(day_rates: list, night_rates: list) -> dict | None:
    """
    Build day/night dict from Economy 7 style dual-register endpoint responses.

    The /day-unit-rates/ and /night-unit-rates/ endpoints return slots with
    valid_from/valid_to as full UTC datetimes (not just time-of-day), so we
    extract the time component from the most recent slot in each list.
    """
    if not day_rates or not night_rates:
        return None

    def _avg_rate(slots):
        dd = [r for r in slots if r.get('payment_method', 'DIRECT_DEBIT') == 'DIRECT_DEBIT']
        src = dd if dd else slots
        vals = [float(r['value_inc_vat']) for r in src if r.get('value_inc_vat') is not None]
        return sum(vals) / len(vals) if vals else None

    def _time_of(iso_str):
        """Extract HH:MM from an ISO datetime string."""
        if not iso_str:
            return None
        return iso_str[11:16]

    avg_day   = _avg_rate(day_rates)
    avg_night = _avg_rate(night_rates)
    if avg_day is None or avg_night is None:
        return None

    # Use the most recent night slot's valid_from/valid_to for the window
    # Sort by valid_from descending to get the current/most recent slot
    sorted_night = sorted(
        [r for r in night_rates if r.get('valid_from')],
        key=lambda r: r['valid_from'],
        reverse=True
    )
    n0 = sorted_night[0] if sorted_night else night_rates[0]

    night_start = _time_of(n0.get('valid_from')) or '00:00'
    night_end   = _time_of(n0.get('valid_to'))   or '07:00'

    return {
        'day_rate':    round(avg_day,   4),
        'night_rate':  round(avg_night, 4),
        'night_start': night_start,
        'night_end':   night_end,
    }

@app.post("/api/octopus/import-tariff")
def octopus_import_tariff():
    """
    Fetch rates from the Octopus API for a given product + region and add
    the tariff to the current simulation config in memory (and optionally
    persist it to settings.yaml if persist=true is passed).

    Expected JSON body:
      {
        "product_code": "AGILE-24-10-01",
        "tariff_code":  "E-1R-AGILE-24-10-01-C",   // optional — resolved from region if absent
        "gsp_region":   "_C",                        // optional, default "_C"
        "date_from":    "2024-01-01",                // optional — defaults to configured history_range
        "date_to":      "2024-12-31",                // optional — defaults to today
        "persist":      false                        // optional — write to settings.yaml
      }

    On success, the tariff is appended to _tariffs in memory and is
    immediately available for simulation via /run or /simulate.
    """
    global _tariffs
    try:
        body = request.get_json(force=True) or {}

        product_code = body.get("product_code", "").strip()
        if not product_code:
            return jsonify({
                "success": False, 
                "error": "product_code is required"
            }), 400

        gsp_region  = body.get("gsp_region", "_C").strip()
        tariff_code = body.get("tariff_code", "").strip()

        # Resolve tariff code from product detail if not supplied
        if not tariff_code:
            detail      = _octo.get_product_detail(product_code)
            tariff_code = _octo.resolve_tariff_code(detail, gsp_region)
            if not tariff_code:
                return jsonify({
                    "success": False,
                    "error": f"Could not resolve tariff code for {product_code} / {gsp_region}"
                }), 400

        log.info("octopus_import_tariff: product=%s tariff=%s gsp=%s",
                 product_code, tariff_code, gsp_region)

        # Determine date range
        today = _dt.now(_tz_mod.utc)
        if body.get("date_to"):
            date_to = _dt.fromisoformat(body["date_to"]).replace(tzinfo=_tz_mod.utc)
        else:
            date_to = today

        if body.get("date_from"):
            date_from = _dt.fromisoformat(body["date_from"]).replace(tzinfo=_tz_mod.utc)
        else:
            # Fall back to the configured history_range
            days = _parse_history_days(_history_range())
            date_from = _dt(today.year - (days // 365),
                            today.month, today.day, tzinfo=_tz_mod.utc)

        # Fetch rates and standing charges
        rates    = _octo.get_tariff_unit_rates(product_code, tariff_code, date_from, date_to)
        standing = _octo.get_tariff_standing_charges(product_code, tariff_code, date_from, date_to)

        if not rates:
            return jsonify({
                "success": False,
                "error": "No unit rates returned from Octopus API for the requested period"
            }), 502

        # Determine standing charge — use the most recent entry
        standing_charge_p = 0.0
        if standing:
            standing.sort(key=lambda x: x.get("valid_from", ""), reverse=True)
            standing_charge_p = float(standing[0].get("value_inc_vat", 0.0))

        # Build a display name from the product detail
        try:
            detail_name = _octo.get_product_detail(product_code).get("display_name", product_code)
        except Exception:
            detail_name = product_code

        tariff_id   = _slugify(f"octo_{product_code}_{gsp_region}")
        tariff_name = f"{detail_name} ({_GSP_REGION_LABELS.get(gsp_region, gsp_region)})"

        # Classify tariff type using tariff code structure, then API data
        from app.octopus_client import classify_tariff
        tc_type   = classify_tariff(tariff_code)
        day_night = None

        unique_rate_values = set(r.get("value_inc_vat") for r in rates)
        log.info("Tariff classification: code=%s tc_type=%s total_slots=%d unique_rates=%d values=%s",
                 tariff_code, tc_type, len(rates), len(unique_rate_values),
                 sorted(unique_rate_values)[:5])

        if tc_type == 'dual_register':
            day_r     = _octo.get_tariff_day_rates(  product_code, tariff_code, date_from, date_to)
            night_r   = _octo.get_tariff_night_rates(product_code, tariff_code, date_from, date_to)
            log.info("Dual-register: day_slots=%d night_slots=%d", len(day_r), len(night_r))
            day_night = _parse_dual_register_rates(day_r, night_r)
        elif tc_type == 'single_register':
            # Sample first 4 slots so we can see valid_from/valid_to in logs
            for s in rates[:4]:
                log.info("  slot: rate=%.4f from=%s to=%s payment=%s",
                         s.get('value_inc_vat', 0),
                         (s.get('valid_from') or '?')[:19],
                         (s.get('valid_to')   or 'open')[:19],
                         s.get('payment_method', 'none'))
            day_night = _parse_day_night_slots(rates)
            log.info("_parse_day_night_slots result: %s", day_night)
        elif tc_type == 'agile':
            pass

        log.info("Final day_night=%s → will build type=%s",
                 day_night,
                 'octopus_day_night' if day_night else ('octopus_agile' if tc_type == 'agile' else 'octopus_flat/tou'))

        # Build tariff object
        if day_night:
            from app.tariffs import OctopusDayNightTariff
            tariff_cfg = {
                "id":              tariff_id,
                "name":            tariff_name,
                "standing_charge": standing_charge_p,
                "export_rate":     0.0,
                "day_rate":        day_night["day_rate"],
                "night_rate":      day_night["night_rate"],
                "night_start":     day_night["night_start"],
                "night_end":       day_night["night_end"],
                "product_code":    product_code,
                "tariff_code":     tariff_code,
                "gsp_region":      gsp_region,
            }
            new_tariff = OctopusDayNightTariff(tariff_cfg)
        elif tc_type == 'agile' or len(set(r.get("value_inc_vat") for r in rates)) > 1:
            from app.tariffs import OctopusTimeOfUseTariff
            tariff_cfg = {
                "id":              tariff_id,
                "name":            tariff_name,
                "standing_charge": standing_charge_p,
                "export_rate":     0.0,
                "rates":           rates,
                "product_code":    product_code,
                "tariff_code":     tariff_code,
                "gsp_region":      gsp_region,
            }
            new_tariff = OctopusTimeOfUseTariff(tariff_cfg)
        else:
            from app.tariffs import OctopusFlatTariff
            unique_rates = set(r.get("value_inc_vat") for r in rates)
            tariff_cfg = {
                "id":              tariff_id,
                "name":            tariff_name,
                "standing_charge": standing_charge_p,
                "export_rate":     0.0,
                "import_rate":     next(iter(unique_rates), 0.0),
                "product_code":    product_code,
                "tariff_code":     tariff_code,
                "gsp_region":      gsp_region,
            }
            new_tariff = OctopusFlatTariff(tariff_cfg)

        # Replace if already loaded (re-import with new dates), otherwise append
        _tariffs = [t for t in _tariffs if t.id != tariff_id]
        _tariffs.append(new_tariff)

        log.info("Imported Octopus tariff %s (%s) — %d rate slots, standing %.2fp/day",
                 tariff_id, tariff_code, len(rates), standing_charge_p)

        global _config_changed_at
        _save_octo_tariffs()
        _config_changed_at = datetime.now(timezone.utc).isoformat()

        resp = {
            "success":           True,
            "tariff_id":         tariff_id,
            "tariff_code":       tariff_code,
            "product_code":      product_code,
            "gsp_region":        gsp_region,
            "tariff_name":       new_tariff.name,
            "tariff_type":       new_tariff.to_dict()["type"],
            "rate_slots":        len(rates),
            "standing_charge_p": standing_charge_p,
            "date_from":         date_from.date().isoformat(),
            "date_to":           date_to.date().isoformat(),
        }
        if day_night:
            resp["day_rate_p"]   = day_night["day_rate"]
            resp["night_rate_p"] = day_night["night_rate"]
            resp["night_start"]  = day_night["night_start"]
            resp["night_end"]    = day_night["night_end"]
        log.info("Import response: %s", resp)
        return jsonify(resp)

    except requests.exceptions.ConnectionError as e:
        return jsonify({"success": False, "error": f"Could not reach Octopus API: {e}"}), 502
    except requests.exceptions.HTTPError as e:
        return jsonify({"success": False, "error": f"Octopus API error: {e}"}), 502
    except Exception as e:
        log.exception("octopus_import_tariff failed")
        return jsonify({"success": False, "error": str(e)}), 500


@app.delete("/api/octopus/tariff/<tariff_id>")
def octopus_remove_tariff(tariff_id):
    """Remove a previously imported Octopus tariff from the in-memory list."""
    global _tariffs, _config_changed_at
    before = len(_tariffs)
    _tariffs = [t for t in _tariffs if t.id != tariff_id]
    removed = before - len(_tariffs)
    _save_octo_tariffs()
    _config_changed_at = datetime.now(timezone.utc).isoformat()
    return jsonify({"success": True, "removed": removed})

@app.patch("/api/octopus/tariff/<tariff_id>")
def octopus_edit_tariff(tariff_id):
    """
    Update rate/window fields on an imported Octopus tariff (user override).

    Accepted JSON fields (all optional — only supplied fields are changed):
      standing_charge, export_rate,
      import_rate                        (octopus_flat)
      day_rate, night_rate, night_start, night_end   (octopus_day_night)
    """
    global _tariffs, _config_changed_at
    from app.tariffs import (OctopusFlatTariff, OctopusDayNightTariff,
                              OctopusTimeOfUseTariff)
    body = request.get_json(force=True) or {}

    tariff = next((t for t in _tariffs if t.id == tariff_id), None)
    if tariff is None:
        return jsonify({"success": False, "error": f"Tariff not found: {tariff_id}"}), 404

    try:
        if "standing_charge" in body:
            tariff.standing_charge = float(body["standing_charge"])
        if "export_rate" in body:
            tariff._export_rate = float(body["export_rate"])

        if isinstance(tariff, OctopusFlatTariff):
            if "import_rate" in body:
                tariff._import_rate = float(body["import_rate"])

        elif isinstance(tariff, OctopusDayNightTariff):
            from app.tariffs import _parse_time as _pt
            if "day_rate"    in body: tariff._day_rate    = float(body["day_rate"])
            if "night_rate"  in body: tariff._night_rate  = float(body["night_rate"])
            if "night_start" in body: tariff._night_start = _pt(body["night_start"])
            if "night_end"   in body: tariff._night_end   = _pt(body["night_end"])

    except (ValueError, TypeError) as e:
        return jsonify({"success": False, "error": f"Invalid value: {e}"}), 400

    _save_octo_tariffs()
    _config_changed_at = datetime.now(timezone.utc).isoformat()
    return jsonify({"success": True, "tariff": tariff.to_dict()})

@app.post("/api/octopus/import-seg-tariff")
def octopus_import_seg_tariff():
    """
    Fetch SEG export rates from the Octopus API and add as an export tariff.

    Expected JSON body:
      {
        "product_code": "OUTGOING-FIX-12M-19-05-13",
        "tariff_code":  "E-1R-OUTGOING-FIX-12M-19-05-13-B",  // optional
        "gsp_region":   "_B",                                  // optional
        "date_from":    "YYYY-MM-DD",                          // optional
        "date_to":      "YYYY-MM-DD",                          // optional
      }
    """
    global _tariffs, _config_changed_at
    from app.tariffs import OctopusSEGTariff
    try:
        body = request.get_json(force=True) or {}

        product_code = body.get("product_code", "").strip()
        if not product_code:
            return jsonify({"success": False, "error": "product_code is required"}), 400

        gsp_region  = body.get("gsp_region", "_C").strip()
        tariff_code = body.get("tariff_code", "").strip()

        if not tariff_code:
            detail      = _octo.get_product_detail(product_code)
            tariff_code = _octo.resolve_tariff_code(detail, gsp_region)
            if not tariff_code:
                return jsonify({
                    "success": False,
                    "error":   f"Could not resolve tariff code for {product_code} / {gsp_region}",
                }), 400

        today = _dt.now(_tz_mod.utc)
        date_to   = _dt.fromisoformat(body["date_to"]).replace(tzinfo=_tz_mod.utc) \
                    if body.get("date_to") else today
        if body.get("date_from"):
            date_from = _dt.fromisoformat(body["date_from"]).replace(tzinfo=_tz_mod.utc)
        else:
            days      = _parse_history_days(_history_range())
            date_from = _dt(today.year - (days // 365), today.month, today.day, tzinfo=_tz_mod.utc)

        rates = _octo.get_seg_tariff_rates(product_code, tariff_code, date_from, date_to)
        if not rates:
            return jsonify({
                "success": False,
                "error":   "No export rates returned from Octopus API for the requested period",
            }), 502

        # Most recent rate = current SEG rate
        rates.sort(key=lambda x: x.get("valid_from", ""), reverse=True)
        export_rate_p = round(float(rates[0].get("value_inc_vat", 0.0)), 4)

        try:
            detail_name = _octo.get_product_detail(product_code).get("display_name", product_code)
        except Exception:
            detail_name = product_code

        tariff_id   = _slugify(f"octo_seg_{product_code}_{gsp_region}")
        tariff_name = f"{detail_name} SEG ({_GSP_REGION_LABELS.get(gsp_region, gsp_region)})"

        new_tariff = OctopusSEGTariff({
            "id":               tariff_id,
            "name":             tariff_name,
            "standing_charge":  0.0,
            "export_rate":      export_rate_p,
            "product_code":     product_code,
            "tariff_code":      tariff_code,
            "gsp_region":       gsp_region,
            "is_current_export": False,
        })

        _tariffs = [t for t in _tariffs if t.id != tariff_id]
        _tariffs.append(new_tariff)

        _save_octo_tariffs()
        _config_changed_at = datetime.now(timezone.utc).isoformat()

        log.info("Imported Octopus SEG tariff %s (%s) — export rate %.4fp/kWh",
                 tariff_id, tariff_code, export_rate_p)

        return jsonify({
            "success":        True,
            "tariff_id":      tariff_id,
            "tariff_code":    tariff_code,
            "product_code":   product_code,
            "gsp_region":     gsp_region,
            "tariff_name":    tariff_name,
            "tariff_type":    "octopus_seg",
            "export_rate_p":  export_rate_p,
        })

    except requests.exceptions.ConnectionError as e:
        return jsonify({"success": False, "error": f"Could not reach Octopus API: {e}"}), 502
    except requests.exceptions.HTTPError as e:
        return jsonify({"success": False, "error": f"Octopus API error: {e}"}), 502
    except Exception as e:
        log.exception("octopus_import_seg_tariff failed")
        return jsonify({"success": False, "error": str(e)}), 500


@app.patch("/api/octopus/seg-tariff/<tariff_id>/set-current")
def octopus_set_current_export(tariff_id):
    """
    Tag one SEG tariff as the current export tariff. Clears the flag on all
    others. The simulator will use this tariff's export_rate for all exports.
    """
    global _tariffs, _config_changed_at
    from app.tariffs import OctopusSEGTariff
    target = next((t for t in _tariffs if t.id == tariff_id), None)
    if target is None or not isinstance(target, OctopusSEGTariff):
        return jsonify({"success": False, "error": f"SEG tariff not found: {tariff_id}"}), 404

    for t in _tariffs:
        if isinstance(t, OctopusSEGTariff):
            t.is_current_export = (t.id == tariff_id)

    _save_octo_tariffs()
    _config_changed_at = datetime.now(timezone.utc).isoformat()
    log.info("Set current export tariff: %s", tariff_id)
    return jsonify({"success": True, "current_export_tariff_id": tariff_id})

@app.delete("/api/octopus/seg-tariff/<tariff_id>")
def octopus_remove_seg_tariff(tariff_id):
    """Remove a SEG tariff from the in-memory list."""
    global _tariffs, _config_changed_at
    before   = len(_tariffs)
    _tariffs = [t for t in _tariffs if t.id != tariff_id]
    _save_octo_tariffs()
    _config_changed_at = datetime.now(timezone.utc).isoformat()
    return jsonify({"success": True, "removed": before - len(_tariffs)})

@app.delete("/api/octopus/cache")
def octopus_clear_cache():
    """Delete all locally cached Octopus API responses."""
    try:
        removed = _octo.clear_cache()
        return jsonify({"success": True, "files_removed": removed})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.post("/api/octopus/account/test")
def octopus_account_test():
    """
    Validate an Octopus API key against a supplied MPAN without persisting
    anything.  If the api_key in the request body is the sentinel '__saved__',
    the stored key from settings is used instead.

    Expected JSON body:
      { "api_key": "<key or __saved__>", "import_mpan": "...", "import_serial": "..." }
    """
    try:
        body         = request.get_json(force=True) or {}
        import_mpan  = body.get('import_mpan', '').strip()
        import_serial = body.get('import_serial', '').strip()
        api_key      = body.get('api_key', '').strip()

        # Change to — only require MPAN
        if not import_mpan:
            return jsonify({'success': False, 'error': 'import_mpan is required'}), 400

        # Resolve sentinel — use the key stored in settings
        if api_key == '__saved__' or not api_key:
            api_key = _cfg.get('octopus_account', {}).get('api_key', '').strip()
        if not api_key:
            return jsonify({'success': False, 'error': 'No API key provided or saved'}), 400

        meter_point = _octo.test_account_credentials(api_key, import_mpan)

        active_agr = _octo.get_active_tariff_from_agreements(meter_point)

        # Meter-point endpoint sometimes returns empty agreements — fall back
        # to the richer /v1/accounts/ endpoint which includes full agreement history
        if not active_agr:
            account_number = body.get('account_number', '').strip()
            # Also fall back to the saved config if not supplied in the request
            if not account_number:
                account_number = _cfg.get('octopus_account', {}).get('account_number', '').strip()
            log.info("No agreements on meter-point response, trying account API with %s", account_number)
            account_agreements = _octo.get_account_agreements(api_key, account_number)
            synthetic = {"agreements": account_agreements}
            active_agr = _octo.get_active_tariff_from_agreements(synthetic)

        active_code  = active_agr.get('tariff_code', '') if active_agr else ''
        parsed       = _octo.parse_tariff_code(active_code)

        # Also look up SEG export agreements from the account
        account_number = body.get('account_number', '').strip() or \
                         _cfg.get('octopus_account', {}).get('account_number', '').strip()
        seg_agreements = _octo.get_export_agreements(api_key, account_number)
        active_seg     = _octo.get_active_tariff_from_agreements({"agreements": seg_agreements}) \
                         if seg_agreements else None
        active_seg_code = active_seg.get('tariff_code', '') if active_seg else ''
        parsed_seg      = _octo.parse_tariff_code(active_seg_code) or {}

        return jsonify({
            'success':        True,
            'mpan':           meter_point.get('mpan', import_mpan),
            'gsp':            meter_point.get('gsp', ''),
            'profile_class':  meter_point.get('profile_class', ''),
            'active_tariff_code':      active_code,
            'active_product_code':     parsed['product_code'],
            'active_gsp_region':       parsed['gsp_region'] or meter_point.get('gsp', ''),
            'agreements':              meter_point.get('agreements', []),
            'active_seg_tariff_code':  active_seg_code,
            'active_seg_product_code': parsed_seg.get('product_code', ''),
        })

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        if status == 401:
            return jsonify({'success': False, 'error': 'Invalid API key (401 Unauthorised)'}), 200
        if status == 403:
            return jsonify({'success': False, 'error': 'API key does not have access to this MPAN (403 Forbidden)'}), 200
        if status == 404:
            return jsonify({'success': False, 'error': 'MPAN not found (404)'}), 200
        return jsonify({'success': False, 'error': f'Octopus API error: {e}'}), 200
    except Exception as e:
        log.exception('octopus_account_test failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.post("/api/octopus/account/fetch-consumption")
def octopus_fetch_consumption():
    """
    Pull half-hourly consumption data from the Octopus API using the saved
    account credentials and write it to the aggregated data store.

    Optional JSON body:
      { "date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD" }
    """
    try:
        if not has_octopus_account(_cfg):
            return jsonify({
                'success': False,
                'error':   'Octopus account credentials not fully configured — save API key, MPAN, and serial first',
            }), 400

        body = request.get_json(force=True, silent=True) or {}
        date_from, date_to = _extract_date_range(body)

        agg_df, diag = run_octopus_ingest(_cfg, date_from=date_from, date_to=date_to)

        if agg_df.empty:
            return jsonify({'success': False, 'error': diag.get('reason', 'No data returned'), 'diag': diag}), 502

        return jsonify({
            'success':     True,
            'intervals':   diag.get('rows', len(agg_df)),
            'date_from':   diag.get('date_from', ''),
            'date_to':     diag.get('date_to', ''),
            'has_export':  diag.get('has_export', False),
            'data_source': diag.get('data_source', 'octopus_api'),
        })

    except Exception as e:
        log.exception('octopus_fetch_consumption failed')
        return jsonify({'success': False, 'error': str(e)}), 500


def _parse_history_days(history_range: str) -> int:
    """Convert a history_range string like '700d' or '1m' to days."""
    try:
        if history_range.endswith("d"):
            return int(history_range[:-1])
        if history_range.endswith("m"):
            return int(history_range[:-1]) * 30
        if history_range.endswith("y"):
            return int(history_range[:-1]) * 365
    except (ValueError, AttributeError):
        pass
    return 365

@app.get("/api/octopus/consumption")
def octopus_consumption():
    """
    Return half-hourly consumption data from the Octopus API for a given date.

    Query params:
      date   YYYY-MM-DD in Europe/London local time (defaults to today)

    Uses credentials from octopus_account in settings.yaml:
      api_key, import_mpan, import_serial

    Response:
    {
      "success": true,
      "date": "2025-05-12",
      "slots": [
        { "interval_start": "2025-05-12T00:00:00+01:00",
          "interval_end":   "2025-05-12T00:30:00+01:00",
          "consumption_kwh": 0.312 },
        ...
      ]
    }
    """
    import zoneinfo
    from datetime import date as _date_type

    LOCAL_TZ = zoneinfo.ZoneInfo('Europe/London')

    acct = _cfg.get('octopus_account', {})
    api_key = acct.get('api_key', '').strip()
    mpan    = acct.get('import_mpan', '').strip()
    serial  = acct.get('import_serial', '').strip()

    if not api_key or not mpan or not serial:
        return jsonify({
            'success': False,
            'error':   'Octopus account credentials not fully configured (api_key, import_mpan, import_serial required)'
        }), 400

    date_str = request.args.get('date', '').strip()
    try:
        if date_str:
            local_date = _dt.strptime(date_str, '%Y-%m-%d').date()
        else:
            local_date = _dt.now(LOCAL_TZ).date()
    except ValueError:
        return jsonify({'success': False, 'error': f'Invalid date format: {date_str!r} — use YYYY-MM-DD'}), 400

    # Build UTC range that covers the full local day, accounting for BST/GMT
    day_start = _dt(local_date.year, local_date.month, local_date.day,
                    0, 0, 0, tzinfo=LOCAL_TZ)
    day_end   = _dt(local_date.year, local_date.month, local_date.day,
                    23, 59, 59, tzinfo=LOCAL_TZ)

    period_from = day_start.astimezone(_tz_mod.utc)
    period_to   = day_end.astimezone(_tz_mod.utc)

    try:
        raw = _octo.get_consumption(mpan, serial, api_key, period_from, period_to)
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        if status == 401:
            return jsonify({'success': False, 'error': 'Octopus API key invalid (401)'}), 502
        return jsonify({'success': False, 'error': f'Octopus API error: {e}'}), 502
    except Exception as e:
        log.exception('octopus_consumption failed')
        return jsonify({'success': False, 'error': str(e)}), 500

    slots = []
    for item in raw:
        try:
            start = _dt.fromisoformat(item['interval_start'].replace('Z', '+00:00'))
            end   = _dt.fromisoformat(item['interval_end'].replace('Z', '+00:00'))
            slots.append({
                'interval_start':  start.astimezone(LOCAL_TZ).isoformat(),
                'interval_end':    end.astimezone(LOCAL_TZ).isoformat(),
                'consumption_kwh': round(float(item['consumption']), 4),
            })
        except (KeyError, ValueError):
            continue

    return jsonify({
        'success': True,
        'date':    local_date.isoformat(),
        'slots':   slots,
    })

@app.get("/api/octopus/export")
def octopus_export():
    """
    Return half-hourly export data from the Octopus API for a given date.

    Query params:
      date   YYYY-MM-DD in Europe/London local time (defaults to today)

    Uses credentials from octopus_account in settings.yaml:
      api_key, export_mpan, export_serial

    Response:
    {
      "success": true,
      "date": "2026-05-10",
      "slots": [
        { "interval_start": "2026-05-10T00:00:00+01:00",
          "interval_end":   "2026-05-10T00:30:00+01:00",
          "consumption_kwh": 0.142 },
        ...
      ]
    }
    """
    import zoneinfo

    LOCAL_TZ = zoneinfo.ZoneInfo('Europe/London')

    acct       = _cfg.get('octopus_account', {})
    api_key    = acct.get('api_key', '').strip()
    export_mpan   = acct.get('export_mpan', '').strip()
    export_serial = acct.get('export_serial', '').strip()

    if not api_key or not export_mpan or not export_serial:
        return jsonify({
            'success': False,
            'error':   'Octopus export credentials not fully configured (api_key, export_mpan, export_serial required)'
        }), 400

    date_str = request.args.get('date', '').strip()
    try:
        if date_str:
            local_date = _dt.strptime(date_str, '%Y-%m-%d').date()
        else:
            local_date = _dt.now(LOCAL_TZ).date()
    except ValueError:
        return jsonify({'success': False, 'error': f'Invalid date format: {date_str!r} — use YYYY-MM-DD'}), 400

    day_start = _dt(local_date.year, local_date.month, local_date.day,
                    0, 0, 0, tzinfo=LOCAL_TZ)
    day_end   = _dt(local_date.year, local_date.month, local_date.day,
                    23, 59, 59, tzinfo=LOCAL_TZ)

    period_from = day_start.astimezone(_tz_mod.utc)
    period_to   = day_end.astimezone(_tz_mod.utc)

    try:
        raw = _octo.get_consumption(export_mpan, export_serial, api_key, period_from, period_to)
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        if status == 401:
            return jsonify({'success': False, 'error': 'Octopus API key invalid (401)'}), 502
        return jsonify({'success': False, 'error': f'Octopus API error: {e}'}), 502
    except Exception as e:
        log.exception('octopus_export failed')
        return jsonify({'success': False, 'error': str(e)}), 500

    slots = []
    for item in raw:
        try:
            start = _dt.fromisoformat(item['interval_start'].replace('Z', '+00:00'))
            end   = _dt.fromisoformat(item['interval_end'].replace('Z', '+00:00'))
            slots.append({
                'interval_start':  start.astimezone(LOCAL_TZ).isoformat(),
                'interval_end':    end.astimezone(LOCAL_TZ).isoformat(),
                'consumption_kwh': round(float(item['consumption']), 4),
            })
        except (KeyError, ValueError):
            continue

    return jsonify({
        'success': True,
        'date':    local_date.isoformat(),
        'slots':   slots,
    })

def _get_cached_month_slots(mpan: str, serial: str, api_key: str,
                             year: int, month: int,
                             kind: str, local_tz) -> list:
    """
    Return Octopus consumption slots for a full calendar month, using an
    in-memory cache keyed by 'YYYY-MM-<kind>' (kind = 'import' or 'export').

    Cache entries older than _OCTO_CACHE_TTL_HOURS are transparently
    re-fetched. Returns a list of (interval_start_local, kwh) tuples.
    """
    import calendar as _cal
    from datetime import timedelta

    cache_key   = f'{year}-{month:02d}-{kind}'
    now_utc     = _dt.now(_tz_mod.utc)
    cached      = _octo_month_cache.get(cache_key)

    if cached:
        age_hours = (now_utc - cached['fetched_at']).total_seconds() / 3600
        if age_hours < _OCTO_CACHE_TTL_HOURS:
            log.debug('Octopus month cache HIT: %s (age=%.1fh)', cache_key, age_hours)
            return cached['slots']
        log.info('Octopus month cache STALE: %s (age=%.1fh) — re-fetching', cache_key, age_hours)
    else:
        log.info('Octopus month cache MISS: %s — fetching from Octopus API', cache_key)

    days_in_month = _cal.monthrange(year, month)[1]
    import zoneinfo
    LOCAL_TZ = zoneinfo.ZoneInfo('Europe/London')

    m_start = _dt(year, month, 1, 0, 0, 0,
                  tzinfo=LOCAL_TZ).astimezone(_tz_mod.utc)
    m_end   = _dt(year, month, days_in_month, 23, 59, 59,
                  tzinfo=LOCAL_TZ).astimezone(_tz_mod.utc)

    try:
        raw = _octo.get_consumption(mpan, serial, api_key, m_start, m_end)
    except Exception as e:
        log.warning('Octopus fetch failed for %s %s: %s', kind, cache_key, e)
        # Return stale data if available rather than nothing
        if cached:
            log.info('Returning stale cache entry for %s', cache_key)
            return cached['slots']
        return []

    slots = []
    for item in raw:
        try:
            start = _dt.fromisoformat(
                item['interval_start'].replace('Z', '+00:00')
            ).astimezone(LOCAL_TZ)
            slots.append((start, float(item['consumption'])))
        except (KeyError, ValueError):
            continue

    _octo_month_cache[cache_key] = {
        'slots':      slots,
        'fetched_at': now_utc,
    }
    log.info('Octopus month cache SET: %s (%d slots)', cache_key, len(slots))
    return slots

@app.get("/api/energy/cost")
def energy_cost():
    """
    Calculate daily and monthly energy cost using cached Octopus consumption
    data and rates from the loaded Octopus import and SEG tariff objects.

    Query params:
      date   YYYY-MM-DD (Europe/London) — defaults to 2 days ago

    Month data is cached in memory for _OCTO_CACHE_TTL_HOURS hours so that
    switching between dates in the same month is near-instant after the first
    load. All monetary values returned in pence.
    """
    import zoneinfo
    import calendar as _cal
    from app.tariffs import OctopusDayNightTariff, OctopusFlatTariff, OctopusSEGTariff

    LOCAL_TZ = zoneinfo.ZoneInfo('Europe/London')

    # ── Resolve import tariff ─────────────────────────────────────────────
    baseline_id   = _baseline_id()
    import_tariff = None
    if baseline_id:
        import_tariff = next(
            (t for t in _tariffs if t.id == baseline_id
             and isinstance(t, (OctopusDayNightTariff, OctopusFlatTariff))), None
        )
    if import_tariff is None:
        import_tariff = next(
            (t for t in _tariffs
             if isinstance(t, (OctopusDayNightTariff, OctopusFlatTariff))), None
        )
    if import_tariff is None:
        return jsonify({'success': False,
                        'error': 'No import tariff loaded — import an Octopus tariff first'}), 400

    if isinstance(import_tariff, OctopusDayNightTariff):
        day_rate_p   = float(import_tariff._day_rate)
        night_rate_p = float(import_tariff._night_rate)
        night_start  = import_tariff._night_start.strftime('%H:%M')
        night_end    = import_tariff._night_end.strftime('%H:%M')
    else:
        day_rate_p   = float(import_tariff._import_rate)
        night_rate_p = day_rate_p
        night_start  = '00:00'
        night_end    = '00:00'

    standing_charge_p = float(import_tariff.standing_charge)

    # ── Resolve SEG export tariff ─────────────────────────────────────────
    export_rate_p = 0.0
    seg_tariff    = next(
        (t for t in _tariffs
         if isinstance(t, OctopusSEGTariff) and getattr(t, 'is_current_export', False)),
        None
    )
    if seg_tariff is None:
        seg_tariff = next((t for t in _tariffs if isinstance(t, OctopusSEGTariff)), None)
    if seg_tariff:
        export_rate_p = float(seg_tariff._export_rate)

    # ── Parse requested date ──────────────────────────────────────────────
    date_str = request.args.get('date', '').strip()
    try:
        if date_str:
            local_date = _dt.strptime(date_str, '%Y-%m-%d').date()
        else:
            from datetime import timedelta
            local_date = (_dt.now(LOCAL_TZ) - timedelta(days=2)).date()
    except ValueError:
        return jsonify({'success': False,
                        'error': f'Invalid date: {date_str!r} — use YYYY-MM-DD'}), 400

    acct          = _cfg.get('octopus_account', {})
    api_key       = acct.get('api_key', '').strip()
    import_mpan   = acct.get('import_mpan', '').strip()
    import_serial = acct.get('import_serial', '').strip()
    export_mpan   = acct.get('export_mpan', '').strip()
    export_serial = acct.get('export_serial', '').strip()

    if not api_key or not import_mpan or not import_serial:
        return jsonify({'success': False,
                        'error': 'Octopus import credentials not configured'}), 400

    # ── Helpers ───────────────────────────────────────────────────────────
    def _is_night(slot_local: _dt) -> bool:
        ns_h, ns_m = int(night_start[:2]), int(night_start[3:])
        ne_h, ne_m = int(night_end[:2]),   int(night_end[3:])
        t  = slot_local.hour * 60 + slot_local.minute
        ns = ns_h * 60 + ns_m
        ne = ne_h * 60 + ne_m
        if ns < ne:
            return ns <= t < ne
        else:
            return t >= ns or t < ne

    def _cost(import_slots, export_slots):
        import_cost   = sum(
            kwh * (night_rate_p if _is_night(s) else day_rate_p)
            for s, kwh in import_slots
        )
        export_credit = sum(kwh * export_rate_p for _, kwh in export_slots)
        return {
            'import_cost_p':   round(import_cost,   2),
            'export_credit_p': round(export_credit, 2),
            'import_kwh':      round(sum(k for _, k in import_slots), 4),
            'export_kwh':      round(sum(k for _, k in export_slots), 4),
        }

    def _filter_day(slots, date):
        """Filter cached month slots down to a single date."""
        return [(s, k) for s, k in slots if s.date() == date]

    # ── Fetch month data (cached) ─────────────────────────────────────────
    year  = local_date.year
    month = local_date.month

    m_import = _get_cached_month_slots(
        import_mpan, import_serial, api_key, year, month, 'import', LOCAL_TZ)
    m_export = _get_cached_month_slots(
        export_mpan, export_serial, api_key, year, month, 'export', LOCAL_TZ) \
        if export_mpan and export_serial else []

    # ── Daily — filter month cache to the requested date ──────────────────
    d_import = _filter_day(m_import, local_date)
    d_export = _filter_day(m_export, local_date)
    dc       = _cost(d_import, d_export)
    d_net    = round(dc['import_cost_p'] + standing_charge_p - dc['export_credit_p'], 2)

    daily = {
        'date':              local_date.isoformat(),
        'has_data':          len(d_import) > 0,
        'import_cost_p':     dc['import_cost_p'],
        'export_credit_p':   dc['export_credit_p'],
        'standing_charge_p': round(standing_charge_p, 4),
        'net_cost_p':        d_net,
        'import_kwh':        dc['import_kwh'],
        'export_kwh':        dc['export_kwh'],
    }

    # ── Monthly — use full cached month slots ─────────────────────────────
    days_in_month  = _cal.monthrange(year, month)[1]
    mc             = _cost(m_import, m_export)
    days_with_data = len({s.date() for s, _ in m_import})
    month_standing = round(standing_charge_p * days_in_month, 2)
    m_net          = round(mc['import_cost_p'] + month_standing - mc['export_credit_p'], 2)
    last_day_data  = max((s.day for s, _ in m_import), default=0)
    month_name     = _cal.month_name[month]
    month_label    = (f'1\u2013{last_day_data} {month_name} {year}'
                      if last_day_data else f'{month_name} {year}')

    monthly = {
        'month':             f'{year}-{month:02d}',
        'label':             month_label,
        'days_with_data':    days_with_data,
        'days_in_month':     days_in_month,
        'import_cost_p':     mc['import_cost_p'],
        'export_credit_p':   mc['export_credit_p'],
        'standing_charge_p': month_standing,
        'net_cost_p':        m_net,
        'import_kwh':        mc['import_kwh'],
        'export_kwh':        mc['export_kwh'],
    }

    return jsonify({
        'success': True,
        'date':    local_date.isoformat(),
        'rates': {
            'day_rate_p':        round(day_rate_p, 4),
            'night_rate_p':      round(night_rate_p, 4),
            'night_start':       night_start,
            'night_end':         night_end,
            'export_rate_p':     round(export_rate_p, 4),
            'standing_charge_p': round(standing_charge_p, 4),
        },
        'daily':   daily,
        'monthly': monthly,
    })

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

_sa_import_log_buffer: deque = deque(maxlen=2000)
_sa_import_log_lock = threading.Lock()

_SA_UPLOAD_DIR  = '/tmp/sa_webui_upload'
_SA_IMPORT_LOG  = '/app/data/sa_import_last.log'    # full log, every run
_SA_FAILURE_LOG = '/app/data/sa_import_failure.log' # written only on failure, persists until next success


def _sa_upload_path() -> str:
    if not os.path.isdir(_SA_UPLOAD_DIR):
        return None
    zips = [f for f in os.listdir(_SA_UPLOAD_DIR) if f.endswith('.zip')]
    return os.path.join(_SA_UPLOAD_DIR, zips[0]) if zips else None


def _sa_probe_zip_dates(zip_path: str) -> dict:
    """
    Read the .manifest file(s) inside the zip and extract the earliest/latest
    timestamps without spinning up Docker.
    """
    earliest = None
    latest   = None
    file_count = 0
    total_bytes = 0

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
        os.makedirs(os.path.dirname(_SA_IMPORT_LOG), exist_ok=True)
        log_file = open(_SA_IMPORT_LOG, 'w', buffering=1)
    except Exception as e:
        log.warning('Could not open SA import log file %s: %s', _SA_IMPORT_LOG, e)
        log_file = None

    def _write_log(line: str):
        if log_file:
            try:
                log_file.write(line + '\n')
            except Exception:
                pass

    try:
        _write_log(f'# SA import started at {_sa_import_state["started_at"]}')
        _write_log(f'# Command: {" ".join(cmd)}')
        _write_log('')

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
            _write_log(line)

            entry = {'line': line, 'written': _sa_import_state['written']}
            with _sa_import_log_lock:
                _sa_import_log_buffer.append(entry)

        proc.stdout.close()
        rc = proc.wait()

        if rc != 0:
            _sa_import_state['failed']  = True
            _sa_import_state['err_msg'] = f'Process exited with code {rc}'
            _write_log(f'\n# FAILED — exit code {rc}')
            _write_log(f'# Points written before failure: {_sa_import_state["written"]}')
            try:
                if log_file:
                    log_file.flush()
                shutil.copy2(_SA_IMPORT_LOG, _SA_FAILURE_LOG)
                log.error('SA import failed (rc=%d, written=%d). Failure log: %s',
                          rc, _sa_import_state['written'], _SA_FAILURE_LOG)
            except Exception as copy_err:
                log.warning('Could not write failure log: %s', copy_err)
        else:
            _write_log(f'\n# SUCCESS — {_sa_import_state["written"]} points written')
            try:
                if os.path.exists(_SA_FAILURE_LOG):
                    os.remove(_SA_FAILURE_LOG)
            except Exception:
                pass

    except Exception as e:
        _sa_import_state['failed']  = True
        _sa_import_state['err_msg'] = str(e)
        _write_log(f'\n# EXCEPTION: {e}')
        log.exception('SA import subprocess error')

    finally:
        _sa_import_state['done'] = True
        _sa_import_running = False
        if log_file:
            try:
                log_file.close()
            except Exception:
                pass
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


@app.post('/api/sa-import/start')
def sa_import_start():
    """
    Start the import as a fire-and-forget background thread.
    Returns immediately with 200 — the UI polls /api/sa-import/status for progress.

    Accepted JSON body fields:
      mode         "custom" (default) or "full"
                   custom: passes --topics-from-settings pointing at settings.yaml,
                           so only the topics required by the simulator are imported.
                   full:   no topic filter — all mapped Solar Assistant measurements
                           are imported.
      range_start  Optional ISO date string (YYYY-MM-DD).  When supplied, only data
                   on or after this date is imported.
    """
    global _sa_import_running, _sa_import_state

    zip_path = _sa_upload_path()
    if not zip_path:
        return jsonify({'success': False, 'error': 'No backup file uploaded'}), 400

    if _sa_import_running:
        return jsonify({'success': False, 'error': 'An import is already running'}), 409

    if not _sa_import_lock.acquire(blocking=False):
        return jsonify({'success': False, 'error': 'An import is already running'}), 409

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

    body        = request.get_json(force=True, silent=True) or {}
    mode        = (body.get('mode') or 'custom').strip().lower()
    range_start = (body.get('range_start') or '').strip()

    script = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'scripts', 'sa_import.py')
    )
    settings_path = os.path.abspath(_SETTINGS_PATH)

    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'

    # --yes skips the interactive confirmation prompt — essential when running
    # as a non-interactive background subprocess (no TTY, stdin is /dev/null).
    # Without this flag the script reads EOF from stdin and cancels immediately,
    # which is why the import previously completed instantly with 0 records.
    cmd = ['python3', script, zip_path, '--yes']

    # Apply the import mode selected in the UI
    if mode == 'custom':
        # Custom import: restrict to only the topics needed by the simulator,
        # read from the mqtt.topics section of settings.yaml
        cmd += ['--topics-from-settings', settings_path]
        log.info('SA import mode: custom (topics from %s)', settings_path)
    else:
        # Full import: no topic filter — all mapped measurements are written
        log.info('SA import mode: full')

    if range_start:
        cmd += ['--range-start', range_start + 'T00:00:00']

    log.info('SA import starting: %s', ' '.join(cmd))

    t = threading.Thread(target=_sa_run_background, args=(cmd, env), daemon=True)
    t.start()

    return jsonify({'success': True, 'started_at': _sa_import_state['started_at']})


@app.post('/api/sa-import/clear')
def sa_import_clear():
    if os.path.isdir(_SA_UPLOAD_DIR):
        shutil.rmtree(_SA_UPLOAD_DIR)
    return jsonify({'success': True})


@app.get('/api/sa-import/status')
def sa_import_status():
    return jsonify({
        'running':         _sa_import_running,
        'has_upload':      _sa_upload_path() is not None,
        'written':         _sa_import_state['written'],
        'skipped':         _sa_import_state['skipped'],
        'last_line':       _sa_import_state['last_line'],
        'done':            _sa_import_state['done'],
        'failed':          _sa_import_state['failed'],
        'err_msg':         _sa_import_state['err_msg'],
        'started_at':      _sa_import_state['started_at'],
        'has_failure_log': os.path.exists(_SA_FAILURE_LOG),
    })


@app.get('/api/sa-import/log')
def sa_import_log():
    """
    Return the tail of the last import log (or failure log if one exists).
    Query params:
      ?lines=200   number of lines from the tail (default 200, max 5000)
      ?failure=1   request the failure log instead of the last-run log
    """
    want_failure = request.args.get('failure', '0') == '1'
    try:
        n = min(int(request.args.get('lines', 200)), 5000)
    except ValueError:
        n = 200

    path = _SA_FAILURE_LOG if (want_failure and os.path.exists(_SA_FAILURE_LOG)) else _SA_IMPORT_LOG

    if not os.path.exists(path):
        return jsonify({'success': False, 'error': 'No log file found'}), 404

    try:
        with open(path, 'r') as f:
            all_lines = f.readlines()
        tail = [line.rstrip('\n') for line in all_lines[-n:]]
        return jsonify({
            'success':     True,
            'log_file':    path,
            'total_lines': len(all_lines),
            'returned':    len(tail),
            'lines':       tail,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5011)), debug=False)
