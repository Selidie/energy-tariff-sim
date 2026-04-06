"""api.py — Flask REST API for the energy tariff simulator."""
import os
import logging
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from app import config as cfg_module
from app.ingest import run_ingest, load_raw
from app.aggregate import run_aggregate, load_aggregated
from app.tariffs import load_tariffs
from app.simulate import compare_tariffs, simulate_tariff, daily_summary, monthly_summary

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)
_cfg = cfg_module.load()
_tariffs = load_tariffs(_cfg)

_UI_PATH = os.path.join(os.path.dirname(__file__), 'ui.html')


@app.get('/')
def ui():
    return send_file(_UI_PATH)


@app.get('/health')
def health():
    return jsonify({'status': 'ok', 'tariffs': [t.id for t in _tariffs]})


@app.post('/ingest')
def ingest():
    global _cfg
    _cfg = cfg_module.load()
    raw_df = run_ingest(_cfg)
    if raw_df.empty:
        return jsonify({'success': False, 'error': 'No data from mqtt-bridge'}), 502
    return jsonify({'success': True, 'rows': len(raw_df),
                    'from': str(raw_df.index.min()), 'to': str(raw_df.index.max())})


@app.post('/aggregate')
def aggregate():
    agg_df = run_aggregate(_cfg)
    if agg_df.empty:
        return jsonify({'success': False, 'error': 'No raw data'}), 400
    return jsonify({'success': True, 'intervals': len(agg_df),
                    'from': str(agg_df.index.min()), 'to': str(agg_df.index.max())})


@app.post('/run')
def run_all():
    global _cfg, _tariffs
    _cfg = cfg_module.load()
    _tariffs = load_tariffs(_cfg)
    raw_df = run_ingest(_cfg)
    if raw_df.empty:
        return jsonify({'success': False, 'error': 'Ingest returned no data'}), 502
    agg_df = run_aggregate(_cfg, raw_df)
    if agg_df.empty:
        return jsonify({'success': False, 'error': 'Aggregation failed'}), 500
    result = compare_tariffs(agg_df, _tariffs)
    return jsonify({'success': True, **result})


@app.get('/simulate')
def simulate():
    agg_df = load_aggregated(_cfg['storage']['aggregated_path'])
    if agg_df.empty:
        return jsonify({'success': False, 'error': 'No aggregated data — POST /aggregate first'}), 400
    result = compare_tariffs(agg_df, _tariffs)
    return jsonify({'success': True, **result})


@app.get('/compare')
def compare():
    agg_df = load_aggregated(_cfg['storage']['aggregated_path'])
    if agg_df.empty:
        return jsonify({'success': False, 'error': 'No aggregated data'}), 400
    result = compare_tariffs(agg_df, _tariffs)
    return jsonify({'success': True, 'comparison': result['comparison'],
                    'baseline_id': result['baseline_id']})


@app.get('/results/daily')
def results_daily():
    tariff_id = request.args.get('tariff')
    tariff = _get_tariff(tariff_id)
    if not tariff:
        return jsonify({'success': False, 'error': f'Unknown tariff: {tariff_id}'}), 404
    agg_df = load_aggregated(_cfg['storage']['aggregated_path'])
    if agg_df.empty:
        return jsonify({'success': False, 'error': 'No aggregated data'}), 400
    detail = simulate_tariff(agg_df, tariff)
    daily = daily_summary(detail).reset_index().to_dict(orient='records')
    return jsonify({'success': True, 'tariff_id': tariff.id, 'daily': daily})


@app.get('/results/monthly')
def results_monthly():
    tariff_id = request.args.get('tariff')
    tariff = _get_tariff(tariff_id)
    if not tariff:
        return jsonify({'success': False, 'error': f'Unknown tariff: {tariff_id}'}), 404
    agg_df = load_aggregated(_cfg['storage']['aggregated_path'])
    if agg_df.empty:
        return jsonify({'success': False, 'error': 'No aggregated data'}), 400
    detail = simulate_tariff(agg_df, tariff)
    daily = daily_summary(detail)
    monthly = monthly_summary(daily).reset_index().to_dict(orient='records')
    return jsonify({'success': True, 'tariff_id': tariff.id, 'monthly': monthly})


@app.get('/tariffs')
def list_tariffs():
    return jsonify({'success': True, 'tariffs': [t.to_dict() for t in _tariffs]})


def _get_tariff(tariff_id):
    if not tariff_id:
        return _tariffs[0] if _tariffs else None
    return next((t for t in _tariffs if t.id == tariff_id), None)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5010)), debug=False)
