#!/bin/bash
# sa_probe.sh - manually start the container and probe what InfluxDB can see
# Run this while the container is still up (use --keep-container on sa_import.py)

PORT=18086
CONTAINER=sa_import_influx1

echo "=== Full container logs ==="
docker logs $CONTAINER 2>&1

echo ""
echo "=== SHOW DATABASES ==="
curl -s "http://localhost:$PORT/query?q=SHOW+DATABASES" | python3 -m json.tool

echo ""
echo "=== SHOW SHARDS ==="
curl -s "http://localhost:$PORT/query?q=SHOW+SHARDS" | python3 -m json.tool

echo ""
echo "=== SHOW SERIES (Battery voltage) ==="
curl -s "http://localhost:$PORT/query?db=solar_assistant&q=SHOW+SERIES+FROM+%22Battery+voltage%22" | python3 -m json.tool

echo ""
echo "=== SELECT 1 row from Battery voltage ==="
curl -s "http://localhost:$PORT/query?db=solar_assistant&epoch=ns&q=SELECT+%2A+FROM+%22Battery+voltage%22+LIMIT+1" | python3 -m json.tool

echo ""
echo "=== Files inside container data dir ==="
docker exec $CONTAINER find /var/lib/influxdb/data/solar_assistant -type f | head -20

echo ""
echo "=== Meta dir contents ==="
docker exec $CONTAINER find /var/lib/influxdb/meta -type f

echo ""
echo "=== influx CLI: SHOW FIELD KEYS ==="
docker exec $CONTAINER influx -host localhost -port 8086 -database solar_assistant -execute "SHOW FIELD KEYS" 2>&1 | head -20

echo ""
echo "=== influx CLI: SELECT 1 row ==="
docker exec $CONTAINER influx -host localhost -port 8086 -database solar_assistant -execute 'SELECT * FROM "Battery voltage" LIMIT 1' 2>&1
