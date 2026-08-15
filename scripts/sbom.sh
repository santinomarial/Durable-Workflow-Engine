#!/bin/sh
set -eu

output=${1:-dist/sbom.cdx.json}
requirements=$(mktemp "${TMPDIR:-/tmp}/dwe-runtime-requirements.XXXXXX")
cleanup() {
  rm -f -- "$requirements"
}
trap cleanup EXIT HUP INT TERM

uv export \
  --frozen \
  --no-dev \
  --no-emit-project \
  --output-file "$requirements" \
  >/dev/null

pip-audit \
  --strict \
  --require-hashes \
  --disable-pip \
  --cache-dir "${PIP_AUDIT_CACHE_DIR:-${TMPDIR:-/tmp}/dwe-pip-audit}" \
  --requirement "$requirements" \
  --format cyclonedx-json \
  --output "$output"
