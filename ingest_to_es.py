#!/usr/bin/env python3
"""
Parquet → ElasticSearch Ingestion Script

Reads all Parquet files from the archive directory and bulk-indexes them
into ElasticSearch for Kibana visualization.

Usage:
    pip install pyarrow elasticsearch
    python3 ingest_to_es.py [--archive-dir ./data/archive] [--es-host localhost:9200]

Creates an index called "weather-status" with proper field mappings.
"""

import argparse
import json
import os
import sys
from pathlib import Path

try:
    import pyarrow.parquet as pq
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    print("Missing dependencies. Install with:")
    print("  pip install pyarrow elasticsearch")
    sys.exit(1)


def create_index(es: Elasticsearch, index_name: str):
    """Create the ES index with explicit field mappings."""
    mapping = {
        "mappings": {
            "properties": {
                "station_id":       {"type": "long"},
                "s_no":             {"type": "long"},
                "battery_status":   {"type": "keyword"},
                "status_timestamp": {"type": "long"},
                "humidity":         {"type": "integer"},
                "temperature":      {"type": "integer"},
                "wind_speed":       {"type": "integer"},
                # Computed at ingestion time for Kibana
                "timestamp_dt":     {"type": "date", "format": "epoch_second"}
            }
        }
    }

    if es.indices.exists(index=index_name):
        print(f"  Index '{index_name}' already exists. Deleting and recreating...")
        es.indices.delete(index=index_name)

    es.indices.create(index=index_name, body=mapping)
    print(f"  Created index '{index_name}' with mappings.")


def read_parquet_files(archive_dir: str):
    """Reads all Parquet files from the archive directory and yields records."""
    archive_path = Path(archive_dir)
    if not archive_path.exists():
        print(f"ERROR: Archive directory not found: {archive_dir}")
        sys.exit(1)

    parquet_files = sorted(archive_path.rglob("*.parquet"))
    if not parquet_files:
        print(f"ERROR: No .parquet files found in {archive_dir}")
        sys.exit(1)

    print(f"  Found {len(parquet_files)} Parquet file(s)")

    total_records = 0
    for pf in parquet_files:
        table = pq.read_table(str(pf))
        df = table.to_pandas()

        for _, row in df.iterrows():
            record = {
                "station_id":       int(row["station_id"]),
                "s_no":             int(row["s_no"]),
                "battery_status":   str(row["battery_status"]),
                "status_timestamp": int(row["status_timestamp"]),
                "humidity":         int(row["humidity"]),
                "temperature":      int(row["temperature"]),
                "wind_speed":       int(row["wind_speed"]),
                # Copy timestamp for Kibana date histogram
                "timestamp_dt":     int(row["status_timestamp"])
            }
            total_records += 1
            yield record

    print(f"  Total records read: {total_records}")


def bulk_index(es: Elasticsearch, index_name: str, records):
    """Bulk-index records into ElasticSearch."""
    def gen_actions():
        for rec in records:
            yield {
                "_index": index_name,
                "_source": rec
            }

    success, errors = helpers.bulk(es, gen_actions(), chunk_size=1000, raise_on_error=False)
    print(f"  Indexed {success} documents, {len(errors) if isinstance(errors, list) else errors} errors")
    return success


def main():
    parser = argparse.ArgumentParser(description="Ingest Parquet files into ElasticSearch")
    parser.add_argument("--archive-dir", default="./data/archive",
                        help="Path to the Parquet archive directory")
    parser.add_argument("--es-host", default="localhost:9200",
                        help="ElasticSearch host:port")
    parser.add_argument("--index", default="weather-status",
                        help="ES index name")
    args = parser.parse_args()

    print(f"=== Parquet → ElasticSearch Ingestion ===")
    print(f"  Archive: {args.archive_dir}")
    print(f"  ES Host: {args.es_host}")
    print(f"  Index:   {args.index}")
    print()

    # Connect to ES
    es = Elasticsearch(f"http://{args.es_host}")
    try:
        info = es.info()
        print(f"  Connected to ElasticSearch {info['version']['number']} ✓")
    except Exception as e:
        print(f"ERROR: Cannot connect to ElasticSearch at {args.es_host}: {e}")
        sys.exit(1)

    # Create index
    create_index(es, args.index)

    # Read and index
    records = read_parquet_files(args.archive_dir)
    bulk_index(es, args.index, records)

    # Verify
    es.indices.refresh(index=args.index)
    count = es.count(index=args.index)["count"]
    print(f"\n  Verification: {count} documents in '{args.index}' index")
    print(f"\n=== Done! Open Kibana at http://localhost:5601 ===")


if __name__ == "__main__":
    main()
