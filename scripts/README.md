# Solar Assistant Import Scripts

These scripts import a Solar Assistant backup (InfluxDB 1.x TSM format) into the
mqtt-bridge InfluxDB 2.x instance used by the energy tariff simulator.

## Files

| File | Purpose |
|------|---------| 
| `sa_inspect.py` | Inspect the raw structure of a backup zip — run this first |
| `sa_import.py` | Main import script — restores backup into InfluxDB 2.x |
| `sa_probe.sh` | Debug tool — probes a running temporary container after a failed or kept import |

---

## Prerequisites

### 1. Docker
Docker must be installed and running on the host machine. The import script pulls and
manages an `influxdb:1.8` container automatically.

```bash
docker --version   # confirm Docker is available
```

### 2. Python package

```bash
pip install influxdb-client --break-system-packages
```

### 3. InfluxDB 2.x credentials

The following environment variables must be set to match your mqtt-bridge `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `INFLUX_URL` | *(required)* | URL of your InfluxDB 2.x instance, e.g. `http://localhost:8086` |
| `INFLUX_TOKEN` | *(required)* | InfluxDB 2.x API token |
| `INFLUX_ORG` | `home` | InfluxDB organisation name |
| `INFLUX_BUCKET` | `solar` | Target bucket for imported data |

---

## Recommended Workflow

### Step 1 — Inspect the backup zip (no Docker needed)

Use `sa_inspect.py` to verify the backup file is valid and understand what's inside
before touching InfluxDB at all:

```bash
python3 sa_inspect.py /path/to/solar_assistant_backup.zip
```

This prints the `.meta`, `.manifest`, and `.tar.gz` shard files inside the zip, along
with their sizes. You should see at least one of each.

---

### Step 2 — Inspect measurements after restore

This starts the temporary InfluxDB 1.x container, restores the backup into it, and
prints every measurement with its field names, topic mapping, and row count — then
exits without writing anything to InfluxDB 2.x:

```bash
python3 sa_import.py /path/to/solar_assistant_backup.zip --inspect
```

Check the output for any measurements labelled `*** NOT MAPPED ***`. Those will be
skipped during import. If you need to add mappings, edit the `MEASUREMENT_MAP` dict
near the top of `sa_import.py`.

---

### Step 3 — Dry run

Preview the full import — shows the first 40 data points that *would* be written, plus
a final count, without touching InfluxDB 2.x:

```bash
python3 sa_import.py /path/to/solar_assistant_backup.zip --dry-run
```

No credentials are needed for `--dry-run` or `--inspect`.

---

### Step 4 — Real import

> ⚠️ **Imports can take several hours.** A typical Solar Assistant installation logging
> at 10-second intervals across 33 measurements generates approximately:
> - **1 day** → ~285,000 data points
> - **1 month** → ~8,500,000 data points  
> - **1 year** → ~102,000,000 data points

```bash
export INFLUX_URL=http://localhost:8086
export INFLUX_TOKEN=your_influxdb_token

python3 sa_import.py /path/to/solar_assistant_backup.zip
```

Or inline:

```bash
INFLUX_URL=http://localhost:8086 \
INFLUX_TOKEN=your_influxdb_token \
python3 sa_import.py /path/to/solar_assistant_backup.zip
```

The script will:
1. Extract the backup zip to a staging directory
2. Pull and start an `influxdb:1.8` Docker container
3. Restore the backup into that container using `influxd restore -portable`
4. Query each measurement and write data points into InfluxDB 2.x with the correct topic names
5. Clean up the container and staging directory on exit

---

## Optional Flags

| Flag | Description |
|------|-------------|
| `--inspect` | Print measurement/field info then exit (no InfluxDB 2.x writes) |
| `--dry-run` | Preview mapping output without writing anything |
| `--range-start DATETIME` | Only import data after this ISO datetime, e.g. `2025-01-01T00:00:00` |
| `--range-end DATETIME` | Only import data before this ISO datetime |
| `--prefix TEXT` | Tag value written to InfluxDB as `prefix` (default: `solar_assistant`) |
| `--batch-size N` | Points per write batch (default: 200) |
| `--write-pause SECS` | Seconds to pause between batches, useful to reduce InfluxDB load (default: 0.0) |
| `--docker-network NAME` | Docker network the temporary container joins (default: `frontend`) |
| `--keep-container` | Leave the temporary container running after the import finishes |

### Importing a date range

Useful for limiting how far back you import, or re-importing a specific period:

```bash
INFLUX_URL=http://localhost:8086 \
INFLUX_TOKEN=your_token \
python3 sa_import.py backup.zip \
  --range-start 2025-01-01T00:00:00
```

The `--range-start` filter is applied inside the InfluxDB 1.x query — only matching
rows are read and written to InfluxDB 2.x. The backup's manifest will still show the
full date range of the file, which is expected.

---

## Topic naming

Imported data is tagged with the same topic names used by the live mqtt-bridge, so
historical and live data coexist seamlessly. Solar Assistant publishes per-inverter
readings which the mqtt-bridge stores under `inverter_1/...` topics:

| Measurement | InfluxDB topic tag |
|-------------|-------------------|
| Grid power | `inverter_1/grid_power/state` |
| PV power | `inverter_1/pv_power/state` |
| Load power | `inverter_1/load_power/state` |
| Battery power | `total/battery_power/state` |
| Battery SOC | `total/battery_state_of_charge/state` |

This matches the schema written by the mqtt-bridge service, so all data appears
under the same topic tags regardless of whether it came from live MQTT or a backup import.

---

## Debugging with sa_probe.sh

If a restore appears to have worked but measurements come back empty, you can keep the
temporary container alive and probe it manually:

```bash
# Run import but leave the container running
python3 sa_import.py backup.zip --inspect --keep-container

# In another terminal, probe the container
bash sa_probe.sh
```

`sa_probe.sh` queries the temporary InfluxDB 1.x container directly via `curl` and the
`influx` CLI, showing databases, shards, field keys, and sample rows.

When you are done, remove the container manually:

```bash
docker rm -f sa_import_influx1
```

---

## Re-importing after a topic name fix

If you previously imported with an older version of `sa_import.py` that used `total/...`
topic names instead of `inverter_1/...`, the old data is orphaned in InfluxDB — the
pipeline will not find it. To fix this:

1. In the InfluxDB UI, go to **Load Data → Buckets**
2. Delete the `solar` bucket and recreate it with the same name and no retention policy
3. Re-run the import — the mqtt-bridge will resume writing live data immediately, and
   the new import will write historical data under the correct topic names

---

## Troubleshooting

**`INFLUX_URL and INFLUX_TOKEN must be set`**
Export the environment variables or use `--dry-run` / `--inspect` which don't need them.

**`influxd restore failed`**
The script automatically tries an offline restore fallback if the RPC restore fails.
Run with `--keep-container` then use `sa_probe.sh` to check what shards were actually
restored into the container.

**Measurements show `*** NOT MAPPED ***`**
Add the missing measurement to the `MEASUREMENT_MAP` dictionary in `sa_import.py`,
following the pattern of the existing entries. Ensure the topic name matches the live
mqtt-bridge topic (i.e. `inverter_1/...` for per-inverter readings).

**Import completes but pipeline still shows zero kWh**
Check that `settings.yaml` topic names match what was imported. Use
`GET /api/bridge/diagnose` on the energy-tariff-sim to verify each configured topic
is found in the bridge's InfluxDB history.
