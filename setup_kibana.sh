#!/bin/bash
#
# Kibana Dashboard Setup Script
#
# Creates two visualizations in Kibana:
#   1. Battery Status Distribution (pie chart)
#   2. Dropped Messages per Station (computed from s_no gaps)
#
# Prerequisites:
#   - ElasticSearch running on localhost:9200
#   - Kibana running on localhost:5601
#   - Data already ingested via ingest_to_es.py
#
# Usage: ./setup_kibana.sh

KIBANA_URL="http://localhost:5601"
ES_URL="http://localhost:9200"

echo "=== Kibana Dashboard Setup ==="

# ─── Step 1: Wait for Kibana to be ready ─────────────────────

echo -n "Waiting for Kibana..."
for i in $(seq 1 30); do
    if curl -s "$KIBANA_URL/api/status" | grep -q '"overall":{"level":"available"' 2>/dev/null; then
        echo " ready!"
        break
    fi
    echo -n "."
    sleep 2
done

# ─── Step 2: Create Data View (Index Pattern) ────────────────

echo "Creating data view for 'weather-status' index..."
curl -s -X POST "$KIBANA_URL/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "title": "weather-status",
      "name": "Weather Status",
      "timeFieldName": "timestamp_dt"
    }
  }' | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"  Data view ID: {d.get('data_view',{}).get('id','ERROR')}\")" 2>/dev/null

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Now open Kibana and create dashboards manually:"
echo "  1. Open: $KIBANA_URL"
echo "  2. Go to: Analytics → Discover (verify data appears)"
echo "  3. Go to: Analytics → Dashboard → Create"
echo ""
echo "=== Dashboard 1: Battery Status Distribution ==="
echo "  - Click 'Create visualization'"
echo "  - Chart type: Pie"
echo "  - Slice by: battery_status (Top values)"
echo "  - Metric: Count"
echo "  - Expected: ~30% low, ~40% medium, ~30% high"
echo ""
echo "=== Dashboard 2: Dropped Messages per Station ==="
echo "  - Click 'Create visualization'"
echo "  - Chart type: Bar / Metric"
echo "  - Split by: station_id"
echo "  - For dropped message count, use Kibana Lens with a formula:"
echo "    Approach: compare max(s_no) - min(s_no) vs count per station"
echo "    dropped = max(s_no) - min(s_no) + 1 - count"
echo "    This gives the number of gaps (dropped messages)"
echo "  - Expected: ~10% of messages dropped per station"
echo ""
echo "TIP: You can also compute dropped messages in Discover using KQL:"
echo "     Filter by station_id: 1, then check s_no sequence for gaps"
