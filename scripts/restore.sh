#!/bin/sh
set -eu

: "${RESTORE_DATABASE_URL:?RESTORE_DATABASE_URL is required}"
: "${DWE_RESTORE_CONFIRM:?Set DWE_RESTORE_CONFIRM=replace-target-database}"

if [ "$DWE_RESTORE_CONFIRM" != "replace-target-database" ]; then
  echo "DWE_RESTORE_CONFIRM must equal replace-target-database" >&2
  exit 2
fi

backup=${1:?Usage: restore.sh BACKUP.dump}
if [ ! -f "$backup" ]; then
  echo "Backup does not exist: $backup" >&2
  exit 2
fi

pg_restore \
  --dbname="$RESTORE_DATABASE_URL" \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges \
  --single-transaction \
  --exit-on-error \
  "$backup"

psql "$RESTORE_DATABASE_URL" \
  --no-psqlrc \
  --tuples-only \
  --command="select 'schema_version=' || max(version) from schema_migrations"
