#!/bin/bash
set -uo pipefail

SYNC="${SYNC:-_build/default/bin/sync.exe}"
DATA_DIR="${DATA_DIR:-data}"
SSH_HOST="${SSH_HOST:-okavango.cl.cam.ac.uk}"
PARALLEL="${PARALLEL:-8}"
CHUNK="${CHUNK:-250}"
SRC_BASE="/data/tessera/v1/global_0.1_degree_representation"
DST_BASE="/mnt/cephfs/tessera/v1/global_0.1_degree_representation"

MANIFEST=$(mktemp)
trap 'rm -f "$MANIFEST"' EXIT

DATA_DIR="$DATA_DIR" "$SYNC" manifest okavango scaleway > "$MANIFEST" 2>/dev/null
COUNT=$(wc -l < "$MANIFEST")
if [ "$COUNT" -eq 0 ]; then
  echo "Nothing to sync."
  exit 0
fi

echo "Copying $COUNT tiles to Scaleway in chunks of $CHUNK..."

start=1
while [ "$start" -le "$COUNT" ]; do
  end=$((start + CHUNK - 1))
  echo "[$start-$end / $COUNT] Copying..."

  sed -n "${start},${end}p" "$MANIFEST" | \
    ssh -o ServerAliveInterval=60 "$SSH_HOST" "xargs -P ${PARALLEL} -I{} bash -c '
      sudo cp -a \"${SRC_BASE}/{}/\" \"${DST_BASE}/{}/\"
    '" \
    || echo "Warning: chunk exited with $?"

  echo "[$start-$end / $COUNT] Recording..."
  sed -n "${start},${end}p" "$MANIFEST" | \
    DATA_DIR="$DATA_DIR" "$SYNC" record scaleway /dev/stdin 2>/dev/null

  start=$((end + 1))
done
echo "Done."
