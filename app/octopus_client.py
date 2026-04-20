"""
octopus_client.py — Lightweight client for the Octopus Energy REST API.

Phase 1 scope: public (unauthenticated) endpoints only.
  - List available electricity products
  - Fetch unit rates for a given tariff + date range
  - Fetch standing charges for a given tariff
  - Local JSON cache to avoid re-fetching the same rates
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests

log = logging.getLogger(__name__)

_BASE_URL   = "https://api.octopus.energy/v1"
_CACHE_DIR  = os.environ.get("OCTOPUS_CACHE_DIR", "/app/data/octopus_cache")
_CACHE_TTL  = int(os.environ.get("OCTOPUS_CACHE_TTL", 3600))   # seconds; default 1 hour
_PAGE_SIZE  = 100
_TIMEOUT    = 15   # seconds per HTTP request


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get(url: str, params: dict = None) -> dict:
    """GET a URL and return parsed JSON, raising on HTTP errors."""
    resp = requests.get(url, params=params, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _paginate(url: str, params: dict = None) -> list:
    """
    Follow Octopus pagination (next link) and return all results combined.
    """
    params = dict(params or {})
    params.setdefault("page_size", _PAGE_SIZE)
    results = []
    next_url = url
    while next_url:
        data     = _get(next_url, params if next_url == url else None)
        results += data.get("results", [])
        next_url = data.get("next")   # None when on the last page
    return results


def _cache_path(key: str) -> str:
    os.makedirs(_CACHE_DIR, exist_ok=True)
    safe = key.replace("/", "_").replace("?", "_").replace("&", "_")
    return os.path.join(_CACHE_DIR, f"{safe}.json")


def _cache_read(key: str) -> Optional[dict]:
    path = _cache_path(key)
    try:
        with open(path) as f:
            entry = json.load(f)
        if time.time() - entry["cached_at"] < _CACHE_TTL:
            return entry["data"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        pass
    return None


def _cache_write(key: str, data) -> None:
    path = _cache_path(key)
    try:
        with open(path, "w") as f:
            json.dump({"cached_at": time.time(), "data": data}, f)
    except Exception as e:
        log.warning("Octopus cache write failed for %s: %s", key, e)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def list_products(is_variable: Optional[bool] = None,
                  is_green: Optional[bool] = None,
                  brand: str = "OCTOPUS_ENERGY") -> list:
    """
    Return a list of available electricity products from the Octopus API.

    Filters to OCTOPUS_ENERGY brand by default to avoid third-party suppliers.
    Pass brand=None to return everything.

    Each product dict contains at minimum:
      code, display_name, full_name, description, is_variable, is_green,
      is_tracker, is_prepay, available_from, available_to, links
    """
    cache_key = f"products_brand_{brand}_var_{is_variable}_green_{is_green}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    params = {}
    if is_variable is not None:
        params["is_variable"] = str(is_variable).lower()
    if is_green is not None:
        params["is_green"] = str(is_green).lower()

    all_products = _paginate(f"{_BASE_URL}/products/", params)

    if brand:
        all_products = [p for p in all_products if p.get("brand") == brand]

    # Keep only electricity products (exclude gas-only)
    electricity = [
        p for p in all_products
        if p.get("direction", "IMPORT") == "IMPORT"
        and not p.get("is_prepay", False)
        and not p.get("is_business", False)
    ]

    _cache_write(cache_key, electricity)
    return electricity


def get_product_detail(product_code: str) -> dict:
    """
    Return full product detail including regional tariff codes,
    standing charges, and links to rate endpoints.
    """
    cache_key = f"product_detail_{product_code}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    data = _get(f"{_BASE_URL}/products/{product_code}/")
    _cache_write(cache_key, data)
    return data


def get_tariff_unit_rates(product_code: str,
                          tariff_code: str,
                          period_from: datetime,
                          period_to: datetime) -> list:
    """
    Fetch electricity unit rates (p/kWh inc VAT) for a tariff over a date range.

    Returns a list of dicts, each with:
      value_inc_vat, valid_from, valid_to

    For flat / day-night tariffs this will be one or a few records.
    For Agile this will be one record per half-hour slot.

    Results are cached per tariff+period combination.
    """
    from_str = period_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str   = period_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    cache_key = f"rates_{tariff_code}_{from_str}_{to_str}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    url    = (f"{_BASE_URL}/products/{product_code}"
              f"/electricity-tariffs/{tariff_code}/standard-unit-rates/")
    params = {"period_from": from_str, "period_to": to_str, "page_size": _PAGE_SIZE}
    rates  = _paginate(url, params)

    _cache_write(cache_key, rates)
    return rates


def get_tariff_standing_charges(product_code: str,
                                tariff_code: str,
                                period_from: datetime,
                                period_to: datetime) -> list:
    """
    Fetch standing charges (p/day inc VAT) for a tariff over a date range.

    For most tariffs this is a single record; some tariffs have had price
    changes so there may be multiple entries.
    """
    from_str = period_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str   = period_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    cache_key = f"standing_{tariff_code}_{from_str}_{to_str}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    url    = (f"{_BASE_URL}/products/{product_code}"
              f"/electricity-tariffs/{tariff_code}/standing-charges/")
    params = {"period_from": from_str, "period_to": to_str, "page_size": _PAGE_SIZE}
    charges = _paginate(url, params)

    _cache_write(cache_key, charges)
    return charges


def resolve_tariff_code(product_detail: dict,
                        gsp_region: str = "_C") -> Optional[str]:
    """
    Extract the direct-debit-monthly single-register electricity tariff code
    for the given GSP region from a product detail response.

    GSP region suffix examples: _A (East), _C (Midlands), _H (South East), etc.
    Defaults to _C (East Midlands / East of England — appropriate for Lincolnshire).
    """
    try:
        regional = product_detail["single_register_electricity_tariffs"]
        entry    = regional.get(gsp_region) or next(iter(regional.values()))
        return entry["direct_debit_monthly"]["code"]
    except (KeyError, StopIteration, TypeError):
        return None


def clear_cache() -> int:
    """Delete all cached files. Returns number of files removed."""
    removed = 0
    try:
        for fname in os.listdir(_CACHE_DIR):
            if fname.endswith(".json"):
                os.remove(os.path.join(_CACHE_DIR, fname))
                removed += 1
    except FileNotFoundError:
        pass
    return removed