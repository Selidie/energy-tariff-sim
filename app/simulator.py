def simulate(df, tariff):
    total_import = 0
    total_export = 0

    for _, row in df.iterrows():
        ts = row["time"]
        total_import += row["import_kwh"] * tariff.import_rate(ts)
        total_export += row["export_kwh"] * tariff.export_rate(ts)

    days = (df["time"].max() - df["time"].min()).days + 1
    standing = tariff.standing_charge() * days

    return round(total_import - total_export + standing, 2)