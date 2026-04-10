"""
simulate.py — Replay aggregated energy data against one or more tariffs.
"""
import logging
import pandas as pd
from app.tariffs import DayNightTariff

log = logging.getLogger(__name__)


def _ensure_datetime_index(df: pd.DataFrame, tz: str = 'UTC') -> pd.DataFrame:
    df = df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    if df.index.tz is None:
        df.index = df.index.tz_localize(tz)
    return df


def simulate_tariff(agg_df: pd.DataFrame, tariff, tz: str = 'UTC') -> pd.DataFrame:
    df = _ensure_datetime_index(agg_df, tz)

    import_rates = pd.Series([tariff.import_rate(dt) for dt in df.index], index=df.index)
    export_rates = pd.Series([tariff.export_rate(dt) for dt in df.index], index=df.index)

    df['import_rate_p']   = import_rates
    df['export_rate_p']   = export_rates
    df['import_cost_p']   = df['grid_import_kwh'] * import_rates
    df['export_credit_p'] = df['grid_export_kwh'] * export_rates

    interval_minutes  = _detect_interval_minutes(df)
    intervals_per_day = (24 * 60) / interval_minutes
    df['standing_p']  = tariff.standing_charge / intervals_per_day
    df['net_cost_p']  = df['import_cost_p'] - df['export_credit_p'] + df['standing_p']

    # ── Day / night split columns (populated for DayNightTariff; zero for flat) ──
    if isinstance(tariff, DayNightTariff):
        is_night = pd.Series(
            [tariff._in_night_period(dt) for dt in df.index], index=df.index
        )
        df['day_import_kwh']   = df['grid_import_kwh'].where(~is_night, 0.0)
        df['night_import_kwh'] = df['grid_import_kwh'].where( is_night, 0.0)
        df['day_import_cost_p']   = df['import_cost_p'].where(~is_night, 0.0)
        df['night_import_cost_p'] = df['import_cost_p'].where( is_night, 0.0)
    else:
        df['day_import_kwh']      = df['grid_import_kwh']
        df['night_import_kwh']    = 0.0
        df['day_import_cost_p']   = df['import_cost_p']
        df['night_import_cost_p'] = 0.0

    return df


def daily_summary(detail_df: pd.DataFrame, tz: str = 'UTC') -> pd.DataFrame:
    df = _ensure_datetime_index(detail_df, tz)
    df = df.copy()
    df['_date'] = df.index.date
    daily = df.groupby('_date').agg(
        import_kwh=('grid_import_kwh',     'sum'),
        export_kwh=('grid_export_kwh',     'sum'),
        day_import_kwh=('day_import_kwh',  'sum'),
        night_import_kwh=('night_import_kwh', 'sum'),
        import_cost_p=('import_cost_p',    'sum'),
        day_import_cost_p=('day_import_cost_p',   'sum'),
        night_import_cost_p=('night_import_cost_p', 'sum'),
        export_credit_p=('export_credit_p','sum'),
        standing_p=('standing_p',          'sum'),
        net_cost_p=('net_cost_p',          'sum'),
    )
    daily['net_cost_gbp'] = daily['net_cost_p'] / 100.0
    return daily


def monthly_summary(daily_df: pd.DataFrame) -> pd.DataFrame:
    monthly = daily_df.copy()
    monthly.index = pd.to_datetime(monthly.index)
    monthly = monthly.resample('ME').sum()
    monthly['net_cost_gbp'] = monthly['net_cost_p'] / 100.0
    monthly.index = monthly.index.to_period('M').to_timestamp()
    return monthly


def yearly_summary(daily_df: pd.DataFrame) -> pd.DataFrame:
    yearly = daily_df.copy()
    yearly.index = pd.to_datetime(yearly.index)
    yearly = yearly.resample('YE').sum()
    yearly['net_cost_gbp'] = yearly['net_cost_p'] / 100.0
    yearly.index = yearly.index.to_period('Y').to_timestamp()
    return yearly


def _serialise_period(df: pd.DataFrame, key: str) -> list:
    records = []
    for ts, row in df.iterrows():
        rec = row.to_dict()
        rec[key] = ts.strftime('%Y-%m-%d')
        records.append(rec)
    return records


def total_summary(detail_df: pd.DataFrame, tariff) -> dict:
    is_day_night = isinstance(tariff, DayNightTariff)
    summary = {
        'tariff_id':               tariff.id,
        'tariff_name':             tariff.name,
        'tariff_type':             'day_night' if is_day_night else 'flat',
        'total_import_kwh':        round(detail_df['grid_import_kwh'].sum(), 3),
        'total_export_kwh':        round(detail_df['grid_export_kwh'].sum(), 3),
        'total_import_cost_p':     round(detail_df['import_cost_p'].sum(), 2),
        'total_export_credit_p':   round(detail_df['export_credit_p'].sum(), 2),
        'total_standing_p':        round(detail_df['standing_p'].sum(), 2),
        'total_net_cost_p':        round(detail_df['net_cost_p'].sum(), 2),
        'total_net_cost_gbp':      round(detail_df['net_cost_p'].sum() / 100.0, 2),
        'interval_count':          len(detail_df),
        'date_from':               str(detail_df.index.min().date()),
        'date_to':                 str(detail_df.index.max().date()),
        # Day / night split (meaningful for day_night tariffs; night_* will be 0 for flat)
        'day_import_kwh':          round(detail_df['day_import_kwh'].sum(), 3),
        'night_import_kwh':        round(detail_df['night_import_kwh'].sum(), 3),
        'day_import_cost_p':       round(detail_df['day_import_cost_p'].sum(), 2),
        'night_import_cost_p':     round(detail_df['night_import_cost_p'].sum(), 2),
    }
    if is_day_night:
        summary['day_rate_p']    = tariff._day_rate
        summary['night_rate_p']  = tariff._night_rate
        summary['night_start']   = tariff._night_start.strftime('%H:%M')
        summary['night_end']     = tariff._night_end.strftime('%H:%M')
        summary['export_rate_p'] = tariff._export_rate
        summary['standing_charge_p'] = tariff.standing_charge
    else:
        summary['flat_rate_p']   = getattr(tariff, '_import_rate', None)
        summary['export_rate_p'] = tariff._export_rate
        summary['standing_charge_p'] = tariff.standing_charge

    return summary


def compare_tariffs(agg_df: pd.DataFrame, tariffs: list, tz: str = 'UTC') -> dict:
    results   = {}
    summaries = []
    for tariff in tariffs:
        detail  = simulate_tariff(agg_df, tariff, tz=tz)
        daily   = daily_summary(detail, tz=tz)
        monthly = monthly_summary(daily)
        yearly  = yearly_summary(daily)
        summary = total_summary(detail, tariff)
        results[tariff.id] = {
            'summary': summary,
            'daily':   daily.reset_index().to_dict(orient='records'),
            'monthly': _serialise_period(monthly, 'month'),
            'yearly':  _serialise_period(yearly,  'year'),
        }
        summaries.append(summary)
        log.info("Simulated %s -> GBP %.2f", tariff.name, summary['total_net_cost_gbp'])

    baseline   = summaries[0] if summaries else None
    comparison = []
    for s in summaries:
        row = dict(s)
        if baseline and s['tariff_id'] != baseline['tariff_id']:
            row['diff_vs_baseline_p']   = round(s['total_net_cost_p']   - baseline['total_net_cost_p'],   2)
            row['diff_vs_baseline_gbp'] = round(s['total_net_cost_gbp'] - baseline['total_net_cost_gbp'], 2)
        else:
            row['diff_vs_baseline_p']   = 0.0
            row['diff_vs_baseline_gbp'] = 0.0
        comparison.append(row)

    return {
        'comparison':  comparison,
        'baseline_id': baseline['tariff_id'] if baseline else None,
        'results':     results,
    }


def _detect_interval_minutes(df: pd.DataFrame) -> int:
    if len(df) < 2:
        return 30
    delta = df.index[1] - df.index[0]
    return int(delta.total_seconds() / 60)
