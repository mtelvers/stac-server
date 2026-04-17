#!/bin/bash
set -euo pipefail

SYNC="${SYNC:-_build/default/bin/sync.exe}"
DATA_DIR="${DATA_DIR:-data}"
SSH_HOST="${SSH_HOST:-okavango.cl.cam.ac.uk}"
PARALLEL="${PARALLEL:-32}"
SRC_BASE="/data/tessera/v1/global_0.1_degree_representation"
DST_BASE="s3://tessera-embeddings/v1/global_0.1_degree_representation"

MANIFEST=$(mktemp)
trap 'rm -f "$MANIFEST"' EXIT

DATA_DIR="$DATA_DIR" "$SYNC" manifest okavango s3 > "$MANIFEST" 2>/dev/null
COUNT=$(wc -l < "$MANIFEST")
echo "Copying $COUNT tiles to S3 ($PARALLEL parallel)..."

cat "$MANIFEST" | \
  ssh "$SSH_HOST" "xargs -P ${PARALLEL} -I{} bash -c '
    AWS_PROFILE=tessera ~/bin/s5cmd cp \
      \"${SRC_BASE}/{}/*\" \
      \"${DST_BASE}/{}/\" && echo \"{}\"
  '"

echo "Updating s3 manifest..."
DATA_DIR="$DATA_DIR" "$SYNC" record s3 "$MANIFEST"
echo "Done."
