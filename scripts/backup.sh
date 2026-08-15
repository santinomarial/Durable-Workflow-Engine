#!/bin/sh
set -eu

umask 077
: "${DATABASE_URL:?DATABASE_URL is required}"

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
output=${1:-"durable-${timestamp}.dump"}
partial="${output}.partial.$$"

cleanup() {
  rm -f -- "$partial"
}
trap cleanup EXIT HUP INT TERM

pg_dump \
  --dbname="$DATABASE_URL" \
  --format=custom \
  --compress=9 \
  --no-owner \
  --no-privileges \
  --file="$partial"

mv -- "$partial" "$output"
trap - EXIT HUP INT TERM

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$output" >"${output}.sha256"
else
  shasum -a 256 "$output" >"${output}.sha256"
fi

echo "backup=$output"
echo "checksum=${output}.sha256"
