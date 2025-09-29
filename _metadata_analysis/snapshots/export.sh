#!/bin/bash

# === Configuration ===
REDIS_PASSWORD="redisdevpwd123"
BACKUP_DATE=$(date +%Y%m%d)   # Format: YYYYMMDD
BACKUP_DIR=$(pwd)

# --- Metrics Storage ---
echo "Backing up metrics-storage-redis..."
docker exec metrics-storage-redis \
  redis-cli -a "$REDIS_PASSWORD" SAVE
docker run --rm \
  -v redis-metrics-data:/source \
  -v "$BACKUP_DIR":/backup \
  alpine cp /source/dump.rdb "/backup/metrics_backup_${BACKUP_DATE}.rdb"

echo "Backups completed:"
echo " - metrics_backup_${BACKUP_DATE}.rdb"
