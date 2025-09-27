#!/bin/bash

# === Configuration ===
REDIS_PASSWORD="redisdevpwd123"
BACKUP_DATE=$(date +%Y%m%d)   # Format: YYYYMMDD
BACKUP_DIR=$(pwd)

echo "Creating Redis backups..."

# --- Intermediate Storage ---
echo "Backing up intermediate-storage-redis..."
docker exec intermediate-storage-redis \
  redis-cli -a "$REDIS_PASSWORD" SAVE
docker run --rm \
  -v redis-intermediate-data:/source \
  -v "$BACKUP_DIR":/backup \
  alpine cp /source/dump.rdb "/backup/intermediate_backup_${BACKUP_DATE}.rdb"

# --- Metrics Storage ---
echo "Backing up metrics-storage-redis..."
docker exec metrics-storage-redis \
  redis-cli -a "$REDIS_PASSWORD" SAVE
docker run --rm \
  -v redis-metrics-data:/source \
  -v "$BACKUP_DIR":/backup \
  alpine cp /source/dump.rdb "/backup/metrics_backup_${BACKUP_DATE}.rdb"

echo "Backups completed:"
echo " - intermediate_backup_${BACKUP_DATE}.rdb"
echo " - metrics_backup_${BACKUP_DATE}.rdb"
