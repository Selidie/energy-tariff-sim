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


def _get_auth(url: str, api_key: str, params: dict = None) -> dict:
    """GET a URL with HTTP Basic Auth (api_key as username, empty password)."""
    resp = requests.get(url, params=params, auth=(api_key, ''), timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _paginate_auth(url: str, api_key: str, params: dict = None) -> list:
    """Paginate an authenticated Octopus endpoint, following next links."""
    params = dict(params or {})
    params.setdefault('page_size', _PAGE_SIZE)
    results = []
    next_url = url
    while next_url:
        data     = _get_auth(next_url, api_key, params if next_url == url else None)
        results += data.get('results', [])
        next_url = data.get('next')
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

def get_tariff_day_rates(product_code: str,
                         tariff_code: str,
                         period_from: datetime,
                         period_to: datetime) -> list:
    """
    Fetch day-rate slots from /day-unit-rates/.
    Only populated for dual-register (E-2R-) tariffs such as Economy 7.
    Returns [] with a 404 for single-register tariffs.
    """
    from_str  = period_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str    = period_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    cache_key = f"day_rates_{tariff_code}_{from_str}_{to_str}"
    cached    = _cache_read(cache_key)
    if cached is not None:
        return cached

    url    = (f"{_BASE_URL}/products/{product_code}"
              f"/electricity-tariffs/{tariff_code}/day-unit-rates/")
    params = {"period_from": from_str, "period_to": to_str, "page_size": _PAGE_SIZE}
    try:
        rates = _paginate(url, params)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return []
        raise
    _cache_write(cache_key, rates)
    return rates


def get_tariff_night_rates(product_code: str,
                           tariff_code: str,
                           period_from: datetime,
                           period_to: datetime) -> list:
    """
    Fetch night-rate slots from /night-unit-rates/.
    Only populated for dual-register (E-2R-) tariffs such as Economy 7.
    Returns [] with a 404 for single-register tariffs.
    """
    from_str  = period_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str    = period_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    cache_key = f"night_rates_{tariff_code}_{from_str}_{to_str}"
    cached    = _cache_read(cache_key)
    if cached is not None:
        return cached

    url    = (f"{_BASE_URL}/products/{product_code}"
              f"/electricity-tariffs/{tariff_code}/night-unit-rates/")
    params = {"period_from": from_str, "period_to": to_str, "page_size": _PAGE_SIZE}
    try:
        rates = _paginate(url, params)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return []
        raise
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


def test_account_credentials(api_key: str, import_mpan: str) -> dict:
    """
    Validate an Octopus API key by fetching the meter point detail for the
    supplied MPAN.  Raises requests.HTTPError on 401 / 403 / 404.

    Returns the raw meter-point JSON dict on success, which includes
    'gsp', 'mpan', 'profile_class' and an 'agreements' list.
    """
    url  = f"{_BASE_URL}/electricity-meter-points/{import_mpan}/"
    data = _get_auth(url, api_key)
    return data

def get_account_agreements(api_key: str, account_number: str) -> list:
    """
    Fetch tariff agreements via the /v1/accounts/{account_number}/ endpoint.

    Returns a list of agreement dicts with tariff_code, valid_from, valid_to,
    or an empty list if the account cannot be fetched or has no agreements.
    """
    if not account_number:
        return []

    try:
        account = _get_auth(f"{_BASE_URL}/accounts/{account_number}/", api_key)
    except Exception as e:
        log.warning("Could not fetch account detail for %s: %s", account_number, e)
        return []

    # Walk properties → electricity meter-points → agreements
    # Exclude export meter points — only return import tariff agreements
    agreements = []
    for prop in account.get("properties", []):
        for meter_point in prop.get("electricity_meter_points", []):
            if meter_point.get("is_export", False):
                continue
            for agr in meter_point.get("agreements", []):
                if agr.get("tariff_code"):
                    agreements.append(agr)

    log.info("Found %d agreement(s) for account %s", len(agreements), account_number)
    return agreements


def get_active_tariff_from_agreements(meter_point: dict) -> Optional[dict]:
    """
    Parse the agreements list from a meter-point response and return the
    currently active tariff agreement, or None if not found.

    Returns a dict with:
      tariff_code  (e.g. "E-1R-AGILE-24-10-01-C")
      valid_from   (ISO string)
      valid_to     (ISO string or None = still active)
    """
    agreements = meter_point.get("agreements", [])
    if not agreements:
        return None

    now = datetime.now(timezone.utc).isoformat()

    # Sort newest first
    sorted_agr = sorted(agreements, key=lambda x: x.get("valid_from", ""), reverse=True)

    # Prefer an agreement with no valid_to (currently active)
    for agr in sorted_agr:
        if agr.get("valid_to") is None:
            return agr

    # Fall back to the most recent one whose valid_to is in the future
    for agr in sorted_agr:
        if agr.get("valid_to", "") > now:
            return agr

    # Last resort: just return the most recent
    return sorted_agr[0] if sorted_agr else None


def parse_tariff_code(tariff_code: str) -> dict:
    """
    Extract product_code and gsp_region from a tariff code string.

    Tariff codes follow the pattern: E-1R-{PRODUCT_CODE}-{GSP}
    e.g. "E-1R-AGILE-24-10-01-C"  →  product_code="AGILE-24-10-01", gsp="_C"

    Returns a dict with keys: product_code, gsp_region (or empty strings if unparseable)
    """
    import re
    m = re.match(r'^[EG]-\dR-(.+)-([A-P])$', tariff_code or "")
    if m:
        return {"product_code": m.group(1), "gsp_region": f"_{m.group(2)}"}
    return {"product_code": "", "gsp_region": ""}

def classify_tariff(tariff_code: str) -> str:
    """
    Determine tariff type from the tariff code structure alone.

    Returns one of:
      'dual_register'  — E-2R-... (Economy 7 style, separate day/night endpoints)
      'single_rate'    — E-1R-... with a fixed single rate (Flexible, Fixed etc.)
      'go_style'       — E-1R-... where standard-unit-rates returns alternating
                         cheap/standard windows (Go, Cosy Octopus, Intelligent Go)
      'agile'          — E-1R-AGILE-... half-hourly variable pricing
      'unknown'        — cannot determine from code alone

    Note: 'go_style' and 'single_rate' both use E-1R- and cannot be
    distinguished from the code alone — the caller must inspect the actual
    rate slots returned (>2 unique time windows = go_style).
    """
    if not tariff_code:
        return 'unknown'
    tc = tariff_code.upper()
    # Dual-register Economy 7 style
    if '-2R-' in tc:
        return 'dual_register'
    # Agile — half-hourly
    if 'AGILE' in tc:
        return 'agile'
    # Single-register — Go/Cosy/Intelligent use E-1R- but return windowed slots
    if '-1R-' in tc:
        return 'single_register'
    return 'unknown'


def get_seg_tariff_rates(product_code: str,
                         tariff_code: str,
                         period_from: datetime,
                         period_to: datetime) -> list:
    """
    Fetch export unit rates for a SEG tariff from /standard-unit-rates/.
    SEG tariffs use the same endpoint as flat import tariffs.
    Returns [] on 404.
    """
    from_str  = period_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str    = period_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    cache_key = f"seg_rates_{tariff_code}_{from_str}_{to_str}"
    cached    = _cache_read(cache_key)
    if cached is not None:
        return cached

    url    = (f"{_BASE_URL}/products/{product_code}"
              f"/electricity-tariffs/{tariff_code}/standard-unit-rates/")
    params = {"period_from": from_str, "period_to": to_str, "page_size": _PAGE_SIZE}
    try:
        rates = _paginate(url, params)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return []
        raise
    _cache_write(cache_key, rates)
    return rates


def get_export_agreements(api_key: str, account_number: str) -> list:
    """
    Fetch SEG export tariff agreements from the account API.
    Returns list of agreement dicts with tariff_code, valid_from, valid_to.
    Filters to export-direction electricity meter points only.
    """
    if not account_number:
        return []
    try:
        account = _get_auth(f"{_BASE_URL}/accounts/{account_number}/", api_key)
    except Exception as e:
        log.warning("Could not fetch account for export agreements %s: %s", account_number, e)
        return []

    agreements = []
    for prop in account.get("properties", []):
        for mp in prop.get("electricity_meter_points", []):
            # Export meter points have is_export=True or contain an export MPAN
            if not mp.get("is_export", False):
                continue
            for agr in mp.get("agreements", []):
                if agr.get("tariff_code"):
                    agreements.append(agr)

    log.info("Found %d SEG export agreement(s) for account %s", len(agreements), account_number)
    return agreements


def get_consumption(mpan: str,
                    serial: str,
                    api_key: str,
                    period_from: datetime,
                    period_to: datetime) -> list:
    """
    Fetch half-hourly consumption data (kWh) for a meter from the Octopus API.

    Returns a list of dicts, each with:
      interval_start  (ISO 8601 string, UTC)
      interval_end    (ISO 8601 string, UTC)
      consumption     (float, kWh)

    Results are ordered oldest-first.  Octopus returns up to 25,000 records
    per page; _paginate_auth follows the next links automatically.
    """
    from_str = period_from.strftime('%Y-%m-%dT%H:%M:%SZ')
    to_str   = period_to.strftime('%Y-%m-%dT%H:%M:%SZ')

    url    = (f"{_BASE_URL}/electricity-meter-points/{mpan}"
              f"/meters/{serial}/consumption/")
    params = {
        'period_from':  from_str,
        'period_to':    to_str,
        'order_by':     'period',
        'page_size':    _PAGE_SIZE,
    }
    return _paginate_auth(url, api_key, params)