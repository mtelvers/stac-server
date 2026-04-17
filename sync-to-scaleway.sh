#!/bin/bash
set -euo pipefail

SYNC="${SYNC:-_build/default/bin/sync.exe}"
DATA_DIR="${DATA_DIR:-data}"
SSH_SRC="${SSH_SRC:-okavango.cl.cam.ac.uk}"
SSH_DST="${SSH_DST:-scaleway}"
PARALLEL="${PARALLEL:-16}"
SRC_BASE="/data/tessera/v1/global_0.1_degree_representation"
DST_BASE="/mnt/cephfs/tessera/v1/global_0.1_degree_representation"

MANIFEST=$(mktemp)
trap 'rm -f "$MANIFEST"' EXIT

DATA_DIR="$DATA_DIR" "$SYNC" manifest okavango scaleway > "$MANIFEST" 2>/dev/null
COUNT=$(wc -l < "$MANIFEST")
echo "Copying $COUNT tiles to Scaleway ($PARALLEL parallel)..."

cat "$MANIFEST" | \
  ssh "$SSH_SRC" "xargs -P ${PARALLEL} -I{} bash -c '
    rsync -a \"${SRC_BASE}/{}/\" \"${SSH_DST}:${DST_BASE}/{}/\" && echo \"{}\"
  '"

echo "Updating scaleway manifest..."
DATA_DIR="$DATA_DIR" "$SYNC" record scaleway "$MANIFEST"
echo "Done."
