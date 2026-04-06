import requests
import pandas as pd
from datetime import datetime

def fetch_history(api_url, topic, range_str="7d"):
    url = f"{api_url}/history?topics={topic}&range={range_str}&window=raw"
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()
    series = data["series"].get(topic, [])
    return pd.DataFrame(series)


def process_grid_power(df):
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df = df.sort_values("time")

    df["delta_s"] = df["time"].diff().dt.total_seconds().fillna(0)

    df["import_w"] = df["value"].clip(lower=0)
    df["export_w"] = (-df["value"].clip(upper=0))

    df["import_kwh"] = (df["import_w"] / 1000) * (df["delta_s"] / 3600)
    df["export_kwh"] = (df["export_w"] / 1000) * (df["delta_s"] / 3600)

    return df


def aggregate(df, interval="30min"):
    df = df.set_index("time")
    agg = df.resample(interval).sum()
    agg = agg[["import_kwh", "export_kwh"]].dropna()
    return agg.reset_index()