#!/bin/sh
set -eu

requirements=$(mktemp "${TMPDIR:-/tmp}/dwe-requirements.XXXXXX")
cleanup() {
  rm -f -- "$requirements"
}
trap cleanup EXIT HUP INT TERM

uv export \
  --frozen \
  --all-groups \
  --no-emit-project \
  --output-file "$requirements" \
  >/dev/null

pip-audit \
  --strict \
  --require-hashes \
  --disable-pip \
  --cache-dir "${PIP_AUDIT_CACHE_DIR:-${TMPDIR:-/tmp}/dwe-pip-audit}" \
  --requirement "$requirements"
