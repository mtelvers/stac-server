# GeoTessera STAC Server

STAC API server and sync tool for managing geospatial embedding tiles across multiple stores.

## Architecture

Every store (okavango, s3, scaleway) is represented by a single parquet file with the same schema: `lat`, `lon`, `year`, `file_size`, `scales_size`, `hash`. The first store listed is the **primary** — its parquet provides the tile index that the STAC API serves. Other stores are cross-referenced against it to track which tiles exist where.

```
data/
├── okavango.parquet   ← primary (upstream registry.parquet)
├── s3.parquet         ← tiles on S3 (built by scan)
└── scaleway.parquet   ← tiles on Scaleway (built by scan)
```

## Quick Start

```bash
docker compose up -d
```

Access at https://stac.mint.caelum.ci.dev

## STAC API Endpoints

- `GET /api/` — Catalog landing page
- `GET /api/conformance` — Conformance classes
- `GET /api/collections` — List collections (one per year)
- `GET /api/collections/{id}` — Single collection
- `GET /api/collections/{id}/items?bbox=&limit=` — Items with spatial filtering
- `GET /api/collections/{id}/items/{id}` — Single item with asset links
- `GET /api/search?bbox=&year=&limit=` — Cross-collection search

## Sync Workflow

### 1. Set up the primary store

The upstream team provides `registry.parquet` listing all tiles on okavango. Use it directly:

```bash
cp registry.parquet data/okavango.parquet
# or symlink:
ln -s /path/to/registry.parquet data/okavango.parquet
```

### 2. Inventory a remote store

Generate a file listing that includes file sizes, then scan it to build a manifest.
The scanner extracts year and `grid_<lon>_<lat>` from path components, and file sizes
from the listing. It distinguishes embeddings from scales files by `_scales.npy`.

```bash
# CephFS / local filesystem
find /mnt/cephfs/tessera/v1/global_0.1_degree_representation \
  -name '*.npy' -printf '%s %p\n' > scaleway_listing.txt

# S3 via s5cmd
s5cmd ls 's3://tessera-embeddings/v1/global_0.1_degree_representation/*/*/*.npy' > s3_listing.txt

# S3 via aws cli
aws s3 ls --recursive s3://tessera-embeddings/v1/global_0.1_degree_representation/ > s3_listing.txt

# Build the manifests
stac-sync scan scaleway scaleway_listing.txt
stac-sync scan s3 s3_listing.txt
```

The scanner handles various listing formats — it takes the last two whitespace-delimited
tokens on each line as `<size> <path>`:
- `108527232 /path/to/2024/grid_0.85_49.95/grid_0.85_49.95.npy` (find -printf)
- `108527232  2024/grid_0.85_49.95/grid_0.85_49.95.npy` (s5cmd ls)
- `2026/04/09 21:34:55  108527232  2017/grid_.../grid_....npy` (aws s3 ls)

Lines without a valid size are skipped.

### 3. Check data quality

```bash
stac-sync check
```

Reports tiles with zero `file_size` or `scales_size` — likely corrupt or incomplete tiles.

### 4. Check sync status

```bash
stac-sync status
```

Shows tile counts per store and pairwise diffs.

### 5. Copy missing tiles

Use the provided sync scripts or build your own from the manifest output:

```bash
# Sync okavango → S3 (runs s5cmd on okavango via SSH, 32 parallel)
./sync-to-s3.sh

# Sync okavango → Scaleway (runs rsync on okavango via SSH, 16 parallel)
./sync-to-scaleway.sh
```

These scripts:
1. Generate a manifest of missing tiles
2. SSH to okavango and copy in parallel
3. Update the target's parquet manifest with `stac-sync record`

For custom workflows, the manifest command outputs one `year/grid_lon_lat` path per line
on stdout (logging goes to stderr):

```bash
stac-sync manifest okavango s3 | \
  ssh okavango.cl.cam.ac.uk 'xargs -P 32 -I{} bash -c "
    AWS_PROFILE=tessera ~/bin/s5cmd cp \
      \"/data/tessera/v1/global_0.1_degree_representation/{}/*\" \
      \"s3://tessera-embeddings/v1/global_0.1_degree_representation/{}/\"
  "'
```

### 6. Restart the server

The server loads parquet files at startup, so restart after updating manifests:

```bash
docker compose restart stac-server
```

## Sync CLI Reference

```
stac-sync ingest <parquet-file> <store-name>
    Copy/normalise a parquet into a store manifest.

stac-sync scan <store-name> <listing-file>
    Build manifest from a file listing with sizes.
    Parses grid coords, year, file_size, and scales_size
    from each line. Recognises *_scales.npy as scale files.

stac-sync diff <source> <target>
    Show tiles present in source but missing from target.

stac-sync manifest <source> <target> [-o file]
    Output paths missing from target (one per line, e.g. 2024/grid_0.85_49.95).

stac-sync record <store-name> <manifest-file>
    Mark tiles as present in a store after copying.

stac-sync check [store-name]
    Data quality check — reports tiles with zero file_size
    or scales_size. Checks all stores if none specified.

stac-sync status
    Overview of all stores, with quality warning if issues found.
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./data` | Directory containing store parquet files |
| `PORT` | `8000` | Server listen port |
| `BASE_URL` | `http://localhost:PORT` | Public base URL for STAC links |
| `STORES` | (required) | Semicolon-separated `name=url` pairs |

## Store URLs

The asset `href` for a tile is built as `<store_base_url>/<year>/<grid_name>`, for example:

```
https://dl2.geotessera.org/v1/global_0.1_degree_representation/2024/grid_0.85_49.95
```

Current stores:

- **okavango**: `https://dl2.geotessera.org/v1/global_0.1_degree_representation`
- **s3**: `https://tessera-embeddings.s3.us-west-2.amazonaws.com/v1/global_0.1_degree_representation`
- **scaleway**: `https://dl1.scw.geotessera.org/v1/global_0.1_degree_representation`

## Development

```bash
opam install . --deps-only
dune build

# Run locally
DATA_DIR=./data PORT=8080 \
  STORES="okavango=https://dl2.geotessera.org/v1/global_0.1_degree_representation" \
  _build/default/bin/server.exe
```
