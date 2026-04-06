"""
simulate.py — Replay aggregated energy data against one or more tariffs.
"""
import logging
import pandas as pd

log = logging.getLogger(__name__)


def simulate_tariff(agg_df: pd.DataFrame, tariff) -> pd.DataFrame:
    df = agg_df.copy()
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    uk_index = df.index.tz_convert('Europe/London')
    import_rates = pd.Series([tariff.import_rate(dt) for dt in uk_index], index=df.index)
    export_rates = pd.Series([tariff.export_rate(dt) for dt in uk_index], index=df.index)
    df['import_rate_p'] = import_rates
    df['export_rate_p'] = export_rates
    df['import_cost_p'] = df['grid_import_kwh'] * import_rates
    df['export_credit_p'] = df['grid_export_kwh'] * export_rates
    interval_minutes = _detect_interval_minutes(df)
    intervals_per_day = (24 * 60) / interval_minutes
    df['standing_p'] = tariff.standing_charge / intervals_per_day
    df['net_cost_p'] = df['import_cost_p'] - df['export_credit_p'] + df['standing_p']
    return df


def daily_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    uk = detail_df.index.tz_convert('Europe/London')
    detail_df = detail_df.copy()
    detail_df['_date'] = uk.date
    daily = detail_df.groupby('_date').agg(
        import_kwh=('grid_import_kwh', 'sum'),
        export_kwh=('grid_export_kwh', 'sum'),
        import_cost_p=('import_cost_p', 'sum'),
        export_credit_p=('export_credit_p', 'sum'),
        standing_p=('standing_p', 'sum'),
        net_cost_p=('net_cost_p', 'sum'),
    )
    daily['net_cost_gbp'] = daily['net_cost_p'] / 100.0
    return daily


def monthly_summary(daily_df: pd.DataFrame) -> pd.DataFrame:
    monthly = daily_df.copy()
    monthly.index = pd.to_datetime(monthly.index)
    monthly = monthly.resample('ME').sum()
    monthly['net_cost_gbp'] = monthly['net_cost_p'] / 100.0
    return monthly


def total_summary(detail_df: pd.DataFrame, tariff) -> dict:
    return {
        'tariff_id': tariff.id,
        'tariff_name': tariff.name,
        'total_import_kwh': round(detail_df['grid_import_kwh'].sum(), 3),
        'total_export_kwh': round(detail_df['grid_export_kwh'].sum(), 3),
        'total_import_cost_p': round(detail_df['import_cost_p'].sum(), 2),
        'total_export_credit_p': round(detail_df['export_credit_p'].sum(), 2),
        'total_standing_p': round(detail_df['standing_p'].sum(), 2),
        'total_net_cost_p': round(detail_df['net_cost_p'].sum(), 2),
        'total_net_cost_gbp': round(detail_df['net_cost_p'].sum() / 100.0, 2),
        'interval_count': len(detail_df),
        'date_from': str(detail_df.index.min().date()),
        'date_to': str(detail_df.index.max().date()),
    }


def compare_tariffs(agg_df: pd.DataFrame, tariffs: list) -> dict:
    results = {}
    summaries = []
    for tariff in tariffs:
        detail = simulate_tariff(agg_df, tariff)
        daily = daily_summary(detail)
        monthly = monthly_summary(daily)
        summary = total_summary(detail, tariff)
        results[tariff.id] = {
            'summary': summary,
            'daily': daily.reset_index().to_dict(orient='records'),
            'monthly': monthly.reset_index().rename(columns={'interval_start': 'month'}).to_dict(orient='records'),
        }
        summaries.append(summary)
        log.info("Simulated %s -> GBP %.2f", tariff.name, summary['total_net_cost_gbp'])

    baseline = summaries[0] if summaries else None
    comparison = []
    for s in summaries:
        row = dict(s)
        if baseline and s['tariff_id'] != baseline['tariff_id']:
            row['diff_vs_baseline_p'] = round(s['total_net_cost_p'] - baseline['total_net_cost_p'], 2)
            row['diff_vs_baseline_gbp'] = round(s['total_net_cost_gbp'] - baseline['total_net_cost_gbp'], 2)
        else:
            row['diff_vs_baseline_p'] = 0.0
            row['diff_vs_baseline_gbp'] = 0.0
        comparison.append(row)

    return {
        'comparison': comparison,
        'baseline_id': baseline['tariff_id'] if baseline else None,
        'results': results,
    }


def _detect_interval_minutes(df: pd.DataFrame) -> int:
    if len(df) < 2:
        return 30
    delta = df.index[1] - df.index[0]
    return int(delta.total_seconds() / 60)
