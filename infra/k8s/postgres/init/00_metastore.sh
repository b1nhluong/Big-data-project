#!/bin/bash
# Tạo DB + user cho Hive Metastore (chạy một lần khi PostgreSQL khởi tạo lần đầu)
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE USER hive WITH PASSWORD '${HIVE_DB_PASSWORD}';
CREATE DATABASE metastore OWNER hive;
SQL
