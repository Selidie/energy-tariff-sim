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
1. Extract the backup zip to a temp directory
2. Pull and start an `influxdb:1.8` Docker container (port 18086 by default)
3. Restore the backup into that container using `influxd restore -portable`
4. Query each measurement and write data points into InfluxDB 2.x
5. Clean up the container and temp directory on exit

---

## Optional Flags

| Flag | Description |
|------|-------------|
| `--inspect` | Print measurement/field info then exit (no InfluxDB 2.x writes) |
| `--dry-run` | Preview mapping output without writing anything |
| `--range-start DATETIME` | Only import data after this ISO datetime, e.g. `2024-01-01T00:00:00` |
| `--range-end DATETIME` | Only import data before this ISO datetime |
| `--prefix TEXT` | Tag value written to InfluxDB as `prefix` (default: `solar_assistant`) |
| `--batch-size N` | Points per write batch (default: 500) |
| `--v1-port PORT` | Host port for the temporary InfluxDB 1.x container (default: 18086) |
| `--keep-container` | Leave the temporary container running after the import finishes |

### Importing a date range

Useful for re-importing a specific period or avoiding duplicate data:

```bash
INFLUX_URL=http://localhost:8086 \
INFLUX_TOKEN=your_token \
python3 sa_import.py backup.zip \
  --range-start 2024-06-01T00:00:00 \
  --range-end   2024-09-01T00:00:00
```

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
`influx` CLI, showing databases, shards, field keys, and sample rows. It connects to
port `18086` and container name `sa_import_influx1` — the defaults used by `sa_import.py`.

When you are done, remove the container manually:

```bash
docker rm -f sa_import_influx1
```

---

## Data Model

Imported data is written to the `solar` measurement in InfluxDB 2.x with these tags:

| Tag | Example value |
|-----|--------------|
| `topic` | `total/battery_power/state` |
| `prefix` | `solar_assistant` |

And one field:

| Field | Type | Description |
|-------|------|-------------|
| `value` | float | The numeric sensor reading |

This matches the schema written by the mqtt-bridge service, so historical backup data
will appear alongside live data in Grafana dashboards without any query changes.

---

## Troubleshooting

**`INFLUX_URL and INFLUX_TOKEN must be set`**
Export the environment variables or use `--dry-run` / `--inspect` which don't need them.

**`influxd restore failed`**
The script automatically tries an offline restore fallback if the RPC restore fails.
Run with `--keep-container` then use `sa_probe.sh` to check what shards were actually
restored into the container.

**Port 18086 already in use**
Use `--v1-port 19086` (or any free port) to pick a different host port for the
temporary container.

**Measurements show `*** NOT MAPPED ***`**
Add the missing measurement to the `MEASUREMENT_MAP` dictionary in `sa_import.py`,
following the pattern of the existing entries.
