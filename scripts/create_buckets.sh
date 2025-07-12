#!/bin/sh
set -e

mc alias set local http://minio:9000 minio miniopass

# Create all required buckets
mc mb --ignore-existing local/ga4-bronze
mc mb --ignore-existing local/ga4-bronze/raw
mc mb --ignore-existing local/ga4-bronze/base_ga4_events
mc mb --ignore-existing local/ga4-bronze/stg_ga4_events

echo "MinIO buckets initialized"
