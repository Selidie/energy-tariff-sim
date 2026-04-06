"""
config.py — Load and validate settings.yaml
"""
import os
import yaml

_DEFAULT_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'settings.yaml')


def load(path: str = None) -> dict:
    path = path or os.environ.get('CONFIG_PATH', _DEFAULT_PATH)
    with open(path, 'r') as f:
        cfg = yaml.safe_load(f)
    _validate(cfg)
    return cfg


def _validate(cfg: dict):
    assert 'mqtt' in cfg and 'api_url' in cfg['mqtt'], "mqtt.api_url required"
    assert 'topics' in cfg['mqtt'], "mqtt.topics required"
    assert 'grid_power' in cfg['mqtt']['topics'], "mqtt.topics.grid_power required"
    for t in cfg.get('tariffs', []):
        assert 'id' in t and 'type' in t, f"Tariff missing id or type: {t}"
        assert t['type'] in ('flat', 'day_night'), f"Unknown tariff type: {t['type']}"
