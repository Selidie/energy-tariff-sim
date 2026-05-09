"""
edf_client.py — Lightweight client for the EDF Energy Kraken REST API.

EDF's open tariff API (https://developer.edfgb-kraken.energy/) exposes the
same Kraken REST surface as the Octopus API, at a different base URL.
This module re-uses all octopus_client internals — only the base URL,
cache namespace, and brand filter differ.
"""
import os
import logging
import requests as _requests
from datetime import datetime
from typing import Optional

# Re-use all low-level helpers from the Octopus client unchanged.
from app.octopus_client import (
    _get, _paginate, _cache_read, _cache_write,
    _PAGE_SIZE, _TIMEOUT,
)

log = logging.getLogger(__name__)

_BASE_URL  = os.environ.get("EDF_API_URL", "https://api.edfgb-kraken.energy/v1")
_CACHE_PFX = "edf_"   # namespaces EDF cache files away from Octopus ones


# ---------------------------------------------------------------------------
# Public API  (mirrors octopus_client surface used by api.py)
# ---------------------------------------------------------------------------

def list_products(brand: str = None) -> list:
    """
    Return available EDF residential electricity import products.

    Filters to non-prepay, non-business import tariffs.
    brand defaults to None — the EDF Kraken API returns its own brand string
    which we log on first call so it can be confirmed and set via EDF_BRAND env var.
    Override via the EDF_BRAND environment variable once the correct value is known.
    """
    brand = brand or os.environ.get("EDF_BRAND") or None
    cache_key = f"{_CACHE_PFX}products_brand_{brand}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    try:
        all_products = _paginate(f"{_BASE_URL}/products/")
    except (_requests.exceptions.ConnectionError,
            _requests.exceptions.Timeout) as e:
        raise RuntimeError(f"Could not reach EDF API at {_BASE_URL}: {e}") from e

    # Log all brand values on first fetch so we can confirm the correct filter
    brands = list({p.get("brand") for p in all_products})
    log.info("EDF API returned %d total products; brands present: %s", len(all_products), brands)

    if brand:
        all_products = [p for p in all_products if p.get("brand") == brand]
        log.info("After brand filter '%s': %d products", brand, len(all_products))

    electricity = [
        p for p in all_products
        if p.get("direction", "IMPORT") == "IMPORT"
        and not p.get("is_prepay", False)
        and not p.get("is_business", False)
    ]

    log.info("EDF electricity products (non-prepay, non-business): %d", len(electricity))
    _cache_write(cache_key, electricity)
    return electricity


def get_product_detail(product_code: str) -> dict:
    cache_key = f"{_CACHE_PFX}product_detail_{product_code}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    try:
        data = _get(f"{_BASE_URL}/products/{product_code}/")
    except (_requests.exceptions.ConnectionError,
            _requests.exceptions.Timeout) as e:
        raise RuntimeError(f"Could not reach EDF API: {e}") from e

    _cache_write(cache_key, data)
    return data


def get_tariff_unit_rates(product_code: str,
                          tariff_code: str,
                          period_from: datetime,
                          period_to: datetime) -> list:
    from_str  = period_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str    = period_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    cache_key = f"{_CACHE_PFX}rates_{tariff_code}_{from_str}_{to_str}"
    cached    = _cache_read(cache_key)
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
    from_str  = period_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str    = period_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    cache_key = f"{_CACHE_PFX}day_rates_{tariff_code}_{from_str}_{to_str}"
    cached    = _cache_read(cache_key)
    if cached is not None:
        return cached

    url    = (f"{_BASE_URL}/products/{product_code}"
              f"/electricity-tariffs/{tariff_code}/day-unit-rates/")
    params = {"period_from": from_str, "period_to": to_str, "page_size": _PAGE_SIZE}
    try:
        rates = _paginate(url, params)
    except _requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return []
        raise
    _cache_write(cache_key, rates)
    return rates


def get_tariff_night_rates(product_code: str,
                           tariff_code: str,
                           period_from: datetime,
                           period_to: datetime) -> list:
    from_str  = period_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str    = period_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    cache_key = f"{_CACHE_PFX}night_rates_{tariff_code}_{from_str}_{to_str}"
    cached    = _cache_read(cache_key)
    if cached is not None:
        return cached

    url    = (f"{_BASE_URL}/products/{product_code}"
              f"/electricity-tariffs/{tariff_code}/night-unit-rates/")
    params = {"period_from": from_str, "period_to": to_str, "page_size": _PAGE_SIZE}
    try:
        rates = _paginate(url, params)
    except _requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return []
        raise
    _cache_write(cache_key, rates)
    return rates


def get_tariff_standing_charges(product_code: str,
                                tariff_code: str,
                                period_from: datetime,
                                period_to: datetime) -> list:
    from_str  = period_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str    = period_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    cache_key = f"{_CACHE_PFX}standing_{tariff_code}_{from_str}_{to_str}"
    cached    = _cache_read(cache_key)
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
    """Extract direct-debit-monthly single-register tariff code for a GSP region."""
    try:
        regional = product_detail["single_register_electricity_tariffs"]
        entry    = regional.get(gsp_region) or next(iter(regional.values()))
        return entry["direct_debit_monthly"]["code"]
    except (KeyError, StopIteration, TypeError):
        return None


def clear_cache() -> int:
    """Delete all EDF-namespaced cache files. Returns number of files removed."""
    from app.octopus_client import _CACHE_DIR
    removed = 0
    try:
        for fname in os.listdir(_CACHE_DIR):
            if fname.startswith(_CACHE_PFX) and fname.endswith(".json"):
                os.remove(os.path.join(_CACHE_DIR, fname))
                removed += 1
    except FileNotFoundError:
        pass
    return removed