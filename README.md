# energy-tariff-sim

Local energy cost simulator that replays real Solar Assistant data against configurable UK tariffs to identify savings.

## Architecture

```
Solar Assistant MQTT → mqtt-bridge /history → ingest → raw Parquet
                                                           ↓
                                                      aggregate (30-min kWh intervals)
                                                           ↓
                                              tariff simulation × N tariffs
                                                           ↓
                                              comparison table + web UI
```

## Quick start

```bash
pip install -r requirements.txt
python -m app.api
# Open http://localhost:5010
```

**Requires:** mqtt-bridge running on port 5003 with InfluxDB enabled (for `/history`).

## Docker

```bash
docker compose up -d
```

Data and config are volume-mounted so they persist across rebuilds.

## Configuration — `config/settings.yaml`

### MQTT / data source
```yaml
mqtt:
  api_url: "http://localhost:5003"
  topics:
    grid_power:    "total/grid_power/state"   # required, W (+import / -export)
    pv_power:      "total/pv_power/state"     # optional
    battery_power: "total/battery_power/state"
    load_power:    "total/load_power/state"
```

### Simulation
```yaml
simulation:
  interval_minutes: 30    # 30 = UK billing standard
  history_range:    "30d" # how far back to pull  (1h 24h 7d 30d)
  history_window:   "1m"  # InfluxDB aggregation  (1m 5m 15m raw)
```

### Tariffs

**Flat rate:**
```yaml
- id: "flat_standard"
  name: "Standard Variable"
  type: "flat"
  standing_charge: 53.37   # pence/day
  import_rate: 24.50       # pence/kWh
  export_rate: 15.00       # pence/kWh
```

**Day/Night (Economy 7):**
```yaml
- id: "economy7"
  name: "Economy 7"
  type: "day_night"
  standing_charge: 53.37
  export_rate: 15.00
  day:
    rate: 28.62
    start: "07:00"
    end:   "00:00"
  night:
    rate: 13.10
    start: "00:00"
    end:   "07:00"
```

Add as many tariffs as you like. The first is the comparison baseline.

## API endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Web UI |
| GET | `/health` | Status check |
| POST | `/run` | Full pipeline: ingest → aggregate → simulate |
| POST | `/ingest` | Fetch from mqtt-bridge, save raw Parquet |
| POST | `/aggregate` | Convert raw → 30-min kWh intervals |
| GET | `/simulate` | Simulate all tariffs against stored data |
| GET | `/compare` | Comparison table only |
| GET | `/results/daily?tariff=<id>` | Daily breakdown |
| GET | `/results/monthly?tariff=<id>` | Monthly breakdown |
| GET | `/tariffs` | List configured tariffs |

## File layout

```
energy-tariff-sim/
├── app/
│   ├── __init__.py
│   ├── api.py          # Flask app + all endpoints
│   ├── config.py       # YAML loader + validation
│   ├── ingest.py       # mqtt-bridge → raw Parquet
│   ├── aggregate.py    # W → kWh, 30-min intervals
│   ├── tariffs.py      # Tariff classes + factory
│   ├── simulate.py     # Cost simulation + comparison
│   └── ui.html         # Self-contained web UI
├── config/
│   └── settings.yaml
├── data/
│   ├── raw/            # Raw Parquet from ingest
│   └── aggregated/     # 30-min interval Parquet
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
