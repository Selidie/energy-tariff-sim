# energy-tariff-sim

Local energy cost simulator that replays real Solar Assistant data against configurable UK tariffs to identify the cheapest deal.

Data is pulled from **mqtt-bridge** (which reads from InfluxDB), converted into 30-minute kWh intervals, and simulated across as many tariffs as you configure. Results are shown in a web UI with daily, monthly, and yearly breakdowns, plus a side-by-side comparison table.

---

## Architecture

```
Solar Assistant MQTT
        │
        ▼
  mqtt-bridge :5003
  (InfluxDB backend)
        │  /history
        ▼
    ingest.py          pulls raw W readings → raw Parquet
        │
        ▼
   aggregate.py        W → 30-min kWh intervals → aggregated Parquet
        │
        ▼
   simulate.py         replay against N tariffs
        │
        ▼
   compare_tariffs()   builds summary + daily/monthly/yearly tables
        │
        ▼
   ui.html / api.py    web UI + REST API  :5011
```

Only **`grid_power`** is required. Positive values = import from grid; negative values = export to grid.

---

## Quick start (local / dev)

```bash
pip install -r requirements.txt
python -m app.api
# Open http://localhost:5011
```

**Requires:** mqtt-bridge running on port 5003 with InfluxDB enabled.

## Docker

```bash
docker compose up -d energy-tariff-sim
```

Data and config are volume-mounted and persist across rebuilds:

```
./energy-tarriff-sim/data/   → /app/data
./energy-tarriff-sim/config/ → /app/config
```

Port **5011** is exposed on the host.

---

## Configuration — `config/settings.yaml`

Settings can be edited directly in the file or via the **Config UI** at `http://localhost:5011/config`.

### MQTT / data source

```yaml
mqtt:
  api_url: "http://mqtt-bridge:5003"
  topics:
    grid_power: "total/grid_power/state"   # required — W, +import / -export
```

Only `grid_power` is required. The mqtt-bridge prefix (e.g. `solar_assistant`) is resolved automatically by the bridge.

### Storage

```yaml
storage:
  raw_path:        /app/data/raw/
  aggregated_path: /app/data/aggregated/
  results_path:    /app/data/results.json
```

### Simulation

```yaml
simulation:
  timezone:           Europe/London   # any valid tz name (used for day/night rate boundaries)
  interval_minutes:   30              # 30 = UK billing standard
  history_range:      700d            # how far back to pull when no date range is selected
  history_window:     1m              # InfluxDB aggregation window (1m 5m 15m)
  baseline_tariff_id: flat_standard   # tariff used as the comparison baseline
```

`history_range` accepts values like `1h`, `24h`, `7d`, `30d`, `365d`, `700d`.

### Tariffs

**Flat rate:**
```yaml
tariffs:
  - id:              flat_standard
    name:            "British Gas"
    type:            flat
    standing_charge: 48.54    # pence/day
    import_rate:     24.39    # pence/kWh
    export_rate:     0.0      # pence/kWh (set to 0 if no SEG)
```

**Day / Night (Economy 7 / smart tariff style):**
```yaml
  - id:              eon_drive
    name:            "EON Next Drive v6"
    type:            day_night
    standing_charge: 56.89    # pence/day
    export_rate:     16.50    # pence/kWh
    day:
      rate:  21.80
      start: "07:00"
      end:   "00:00"
    night:
      rate:  6.70
      start: "00:00"
      end:   "07:00"
```

The night window handles midnight wrap-around automatically (e.g. `00:00–07:00`).  
Add as many tariffs as you like. The tariff whose `id` matches `baseline_tariff_id` is listed first and used as the comparison reference.

---

## Web UI

Open `http://localhost:5011` for the main dashboard.

### Running a simulation

1. Optionally select a **date range** (defaults to the full `history_range` from config)
2. Click **Run** — this ingests fresh data from mqtt-bridge, aggregates it, and simulates all tariffs
3. Results appear immediately: comparison table, and daily/monthly/yearly charts per tariff

### Config UI

Open `http://localhost:5011/config` to edit settings through a form:

- MQTT bridge host and port
- Timezone
- Baseline tariff selection
- Add, edit, or remove tariffs (flat or day/night)

Changes are saved back to `config/settings.yaml` and take effect immediately — no restart needed.

---

## API endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Main web UI |
| GET | `/config` | Config editor UI |
| GET | `/health` | Service status, bridge connectivity, tariff list |
| POST | `/run` | Full pipeline: ingest → aggregate → simulate. Accepts optional `date_from` / `date_to` (JSON body) |
| POST | `/ingest` | Fetch from mqtt-bridge, save raw Parquet. Accepts optional `date_from` / `date_to` |
| POST | `/aggregate` | Convert raw Parquet → 30-min kWh intervals |
| GET | `/simulate` | Re-simulate against stored aggregated data (no new ingest) |
| GET | `/compare` | Comparison table only (no new simulation) |
| GET | `/results/daily?tariff=<id>` | Daily breakdown for one tariff |
| GET | `/results/monthly?tariff=<id>` | Monthly breakdown for one tariff |
| GET | `/results/yearly?tariff=<id>` | Yearly breakdown for one tariff |
| GET | `/tariffs` | List all configured tariffs |
| GET | `/api/config` | Get current settings.yaml as JSON |
| POST | `/api/config` | Save updated settings (reloads config + tariffs live) |
| GET | `/api/results` | Load last saved simulation results from disk |
| DELETE | `/api/results` | Clear saved results |
| GET | `/api/bridge/topics` | All topics seen by mqtt-bridge |
| GET | `/api/bridge/topics/numeric` | Numeric topics only |
| GET | `/api/bridge/diagnose` | Full bridge diagnostic — MQTT status, InfluxDB, topic mapping |

### Date range on `/run` and `/ingest`

```bash
curl -X POST http://localhost:5011/run \
  -H "Content-Type: application/json" \
  -d '{"date_from": "2024-11-01", "date_to": "2025-03-31"}'
```

When `date_from` / `date_to` are supplied they override `history_range`. The bridge is queried for enough history to cover the window and the result is clipped to the exact dates requested.

---

## Results persistence

After every `/run` or `/simulate` call, results are saved to `data/results.json`. When the UI loads it restores the last run automatically — so results survive a page refresh or container restart without needing to re-ingest.

Results are cleared by `DELETE /api/results` or by running a new simulation.

---

## File layout

```
energy-tariff-sim/
├── app/
│   ├── __init__.py
│   ├── api.py          # Flask app + all REST endpoints
│   ├── config.py       # YAML loader + validation
│   ├── ingest.py       # mqtt-bridge /history → raw Parquet
│   ├── aggregate.py    # W readings → 30-min kWh intervals
│   ├── tariffs.py      # FlatTariff, DayNightTariff, factory
│   ├── simulate.py     # Cost simulation, daily/monthly/yearly summaries
│   ├── ui.html         # Main web UI (self-contained)
│   └── config.html     # Config editor UI (self-contained)
├── config/
│   └── settings.yaml   # All runtime configuration
├── data/
│   ├── raw/            # Raw Parquet files from ingest
│   ├── aggregated/     # 30-min interval Parquet files
│   └── results.json    # Last simulation results (persisted)
├── scripts/
│   ├── README.md       # Full Solar Assistant import guide
│   ├── sa_import.py    # Import a Solar Assistant backup into InfluxDB 2.x
│   ├── sa_inspect.py   # Inspect a backup zip without importing
│   └── sa_probe.sh     # Debug a running temporary InfluxDB 1.x container
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Solar Assistant historical data import

If you have a Solar Assistant backup and want to backfill InfluxDB with historical data, use the scripts in `scripts/`. They handle the full InfluxDB 1.x → 2.x migration automatically.

See **[scripts/README.md](scripts/README.md)** for the full workflow.

Quick version:

```bash
cd scripts

# 1. Inspect the backup first
python3 sa_inspect.py /path/to/backup.zip

# 2. Preview what would be imported
python3 sa_import.py /path/to/backup.zip --dry-run

# 3. Import
INFLUX_URL=http://localhost:8086 \
INFLUX_TOKEN=your_token \
python3 sa_import.py /path/to/backup.zip
```

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `flask >= 3.0` | REST API and HTML page serving |
| `flask-cors` | CORS headers for local dev |
| `pandas >= 2.0` | Data manipulation and resampling |
| `pyarrow` | Parquet read/write |
| `pyyaml` | Config file parsing and saving |
| `requests` | HTTP calls to mqtt-bridge |
| `apscheduler` | (available for scheduled runs) |

---

## Troubleshooting

**Bridge check fails at startup**
Confirm mqtt-bridge is running and `mqtt.api_url` in `settings.yaml` resolves correctly. Inside Docker the service name (`mqtt-bridge`) is used; for local dev use `http://localhost:5003`.

**No data returned from ingest**
Run `GET /api/bridge/diagnose` to check MQTT connectivity, InfluxDB status, and whether the `grid_power` topic is being received. The bridge must have been running long enough to accumulate readings.

**Day/night rate boundaries look wrong**
Check `simulation.timezone` in `settings.yaml` matches your local timezone (e.g. `Europe/London`). The simulator applies rate boundaries using wall-clock time in that timezone, so an incorrect timezone will shift night periods by the UTC offset.

**Port conflict**
The container uses port `5011`. Change the host-side port in `docker-compose.yml` if needed:
```yaml
ports:
  - "5999:5011"
```
