#!/usr/bin/env bash
# zfs-backup.sh — ZFS incremental snapshot backup to S3 via s3duct
#
# Usage:
#   zfs-backup.sh [--full] [--rollup N] [--dry-run] pool/dataset [pool/dataset2 ...]
#   zfs-backup.sh --all pool             # back up all child datasets
#   zfs-backup.sh --restore pool/dataset  # list available backups + restore instructions
#   zfs-backup.sh --list                  # list all backups in S3
#
# Environment:
#   S3DUCT_BUCKET       — S3 bucket (required)
#   S3DUCT_KEY          — AES-256-GCM key (hex:..., file:..., env:...)
#   S3DUCT_REGION       — AWS region (optional)
#   S3DUCT_ENDPOINT_URL — Custom endpoint for R2/MinIO/B2 (optional)
#   S3DUCT_CHUNK_SIZE   — Chunk size (default: 512M)
#   S3DUCT_ROLLUP       — Auto-rollup threshold (default: 30, override with --rollup)
#   S3DUCT_PREFIX       — S3 key prefix (optional, for multi-tenant buckets)
#
# Stream naming convention:
#   zfs/<dataset>/full-<snapshot>          — full base send
#   zfs/<dataset>/incr-<base>-<snapshot>   — incremental from base to snapshot
#
# Tags stored with each stream:
#   dataset=pool/data  type=full|incremental  base=<snap>  snapshot=<snap>
#
# Snapshots are named: s3duct-auto-YYYYMMDD-HHMMSS
#
# Restore order:
#   1. Restore the full base: s3duct get ... | zfs recv pool/dataset
#   2. Apply incrementals in order: s3duct get ... | zfs recv pool/dataset

set -euo pipefail

# --- defaults ---
BUCKET="${S3DUCT_BUCKET:?Set S3DUCT_BUCKET}"
KEY="${S3DUCT_KEY:-}"
REGION="${S3DUCT_REGION:-}"
ENDPOINT="${S3DUCT_ENDPOINT_URL:-}"
CHUNK_SIZE="${S3DUCT_CHUNK_SIZE:-512M}"
ROLLUP_THRESHOLD="${S3DUCT_ROLLUP:-30}"
PREFIX="${S3DUCT_PREFIX:-}"
SNAP_PREFIX="s3duct-auto"
DRY_RUN=false
FORCE_FULL=false
MODE="backup"
ALL_CHILDREN=false
DATASETS=()

# --- helpers ---
die()  { echo "ERROR: $*" >&2; exit 1; }
info() { echo "[$(date +%H:%M:%S)] $*" >&2; }

s3duct_opts() {
    local opts="--bucket ${BUCKET}"
    [[ -n "$KEY" ]]      && opts+=" --key ${KEY}"
    [[ -n "$REGION" ]]   && opts+=" --region ${REGION}"
    [[ -n "$ENDPOINT" ]] && opts+=" --endpoint-url ${ENDPOINT}"
    [[ -n "$PREFIX" ]]   && opts+=" --prefix ${PREFIX}"
    echo "$opts"
}

# Sanitize dataset name for use in S3 stream names (replace / with -)
sanitize() { echo "$1" | tr '/' '-'; }

# Stream name for a full backup
stream_full() { echo "zfs/$(sanitize "$1")/full-$2"; }

# Stream name for an incremental backup
stream_incr() { echo "zfs/$(sanitize "$1")/incr-$2-$3"; }

# Generate a timestamp-based snapshot name
snap_name() { echo "${SNAP_PREFIX}-$(date -u +%Y%m%d-%H%M%S)"; }

# Find the latest s3duct-auto snapshot on a dataset
latest_snap() {
    local ds="$1"
    zfs list -t snapshot -o name -s creation -H "$ds" 2>/dev/null \
        | grep "@${SNAP_PREFIX}-" \
        | tail -1 \
        | cut -d@ -f2
}

# Find the current base snapshot (the one used for the latest full backup)
# We store this in a ZFS user property: s3duct:base
get_base() {
    local ds="$1"
    zfs get -H -o value s3duct:base "$ds" 2>/dev/null | grep -v '^-$' || true
}

set_base() {
    local ds="$1" snap="$2"
    zfs set "s3duct:base=$snap" "$ds"
}

# Count incrementals since last full
count_incrementals() {
    local ds="$1" base="$2"
    # Count s3duct-auto snapshots created after the base
    local base_ts
    base_ts=$(zfs get -H -o value -p creation "${ds}@${base}" 2>/dev/null || echo 0)
    zfs list -t snapshot -o name,creation -s creation -H "$ds" 2>/dev/null \
        | grep "@${SNAP_PREFIX}-" \
        | while IFS=$'\t' read -r name _; do
            local snap_name
            snap_name=$(echo "$name" | cut -d@ -f2)
            local snap_ts
            snap_ts=$(zfs get -H -o value -p creation "$name" 2>/dev/null || echo 0)
            if [[ "$snap_ts" -gt "$base_ts" && "$snap_name" != "$base" ]]; then
                echo "$snap_name"
            fi
        done | wc -l
}

# --- backup logic ---

do_full() {
    local ds="$1"
    local snap
    snap=$(snap_name)
    local stream
    stream=$(stream_full "$ds" "$snap")

    info "Creating snapshot ${ds}@${snap}"
    $DRY_RUN && { info "[dry-run] Would create ${ds}@${snap} and send full to ${stream}"; return; }

    zfs snapshot "${ds}@${snap}"

    info "Sending full backup: ${ds}@${snap} -> ${stream}"
    zfs send -cp "${ds}@${snap}" \
        | s3duct put $(s3duct_opts) \
            --name "$stream" \
            --chunk-size "$CHUNK_SIZE" \
            --tag "dataset=${ds}" \
            --tag "type=full" \
            --tag "base=${snap}" \
            --tag "snapshot=${snap}"

    set_base "$ds" "$snap"
    info "Full backup complete: ${stream}"
}

do_incremental() {
    local ds="$1"
    local base="$2"
    local snap
    snap=$(snap_name)
    local stream
    stream=$(stream_incr "$ds" "$base" "$snap")

    info "Creating snapshot ${ds}@${snap}"
    $DRY_RUN && { info "[dry-run] Would create ${ds}@${snap} and send incremental (base: ${base}) to ${stream}"; return; }

    zfs snapshot "${ds}@${snap}"

    info "Sending incremental: ${ds}@${base} -> ${ds}@${snap} as ${stream}"
    zfs send -cp -i "${ds}@${base}" "${ds}@${snap}" \
        | s3duct put $(s3duct_opts) \
            --name "$stream" \
            --chunk-size "$CHUNK_SIZE" \
            --tag "dataset=${ds}" \
            --tag "type=incremental" \
            --tag "base=${base}" \
            --tag "snapshot=${snap}"

    info "Incremental backup complete: ${stream}"
}

backup_dataset() {
    local ds="$1"

    # Check dataset exists
    zfs list "$ds" >/dev/null 2>&1 || die "Dataset not found: ${ds}"

    local base
    base=$(get_base "$ds")

    if $FORCE_FULL || [[ -z "$base" ]]; then
        info "Taking full backup of ${ds} (${FORCE_FULL:+forced}${base:+no base found})"
        do_full "$ds"
        return
    fi

    # Verify base snapshot still exists
    if ! zfs list -t snapshot "${ds}@${base}" >/dev/null 2>&1; then
        info "Base snapshot ${ds}@${base} no longer exists, taking new full backup"
        do_full "$ds"
        return
    fi

    # Check rollup threshold
    local count
    count=$(count_incrementals "$ds" "$base")
    if [[ "$count" -ge "$ROLLUP_THRESHOLD" ]]; then
        info "Rollup threshold reached (${count} >= ${ROLLUP_THRESHOLD}), taking new full backup"
        do_full "$ds"
        return
    fi

    info "Taking incremental backup of ${ds} (base: ${base}, incremental #$((count + 1)))"
    do_incremental "$ds" "$base"
}

# --- restore / list ---

do_list() {
    info "Listing backups in s3://${BUCKET}"
    s3duct list $(s3duct_opts)
}

do_restore_info() {
    local ds="$1"
    local sanitized
    sanitized=$(sanitize "$ds")

    echo ""
    echo "=== Restore instructions for ${ds} ==="
    echo ""
    echo "Available backups:"
    s3duct list $(s3duct_opts) 2>/dev/null | grep "zfs/${sanitized}/" || {
        echo "  No backups found for ${ds}"
        return
    }

    echo ""
    echo "To restore, apply in order:"
    echo ""
    echo "  1. Restore the full base:"
    echo "     s3duct get $(s3duct_opts) --name 'zfs/${sanitized}/full-<SNAPSHOT>' | zfs recv ${ds}"
    echo ""
    echo "  2. Apply each incremental in chronological order:"
    echo "     s3duct get $(s3duct_opts) --name 'zfs/${sanitized}/incr-<BASE>-<SNAP>' | zfs recv ${ds}"
    echo ""
    echo "  The incrementals must be applied in the order they were created."
    echo "  The stream names are sorted chronologically by default."
    echo ""
}

# --- arg parsing ---

while [[ $# -gt 0 ]]; do
    case "$1" in
        --full)      FORCE_FULL=true; shift ;;
        --dry-run)   DRY_RUN=true; shift ;;
        --rollup)    ROLLUP_THRESHOLD="$2"; shift 2 ;;
        --all)       ALL_CHILDREN=true; shift ;;
        --list)      MODE="list"; shift ;;
        --restore)   MODE="restore"; shift ;;
        --help|-h)
            echo "Usage: $0 [--full] [--rollup N] [--dry-run] [--all] dataset [dataset ...]"
            echo "       $0 --list"
            echo "       $0 --restore dataset"
            exit 0
            ;;
        -*)          die "Unknown option: $1" ;;
        *)           DATASETS+=("$1"); shift ;;
    esac
done

# --- main ---

case "$MODE" in
    list)
        do_list
        ;;
    restore)
        [[ ${#DATASETS[@]} -eq 0 ]] && die "Specify a dataset for --restore"
        for ds in "${DATASETS[@]}"; do
            do_restore_info "$ds"
        done
        ;;
    backup)
        if $ALL_CHILDREN; then
            [[ ${#DATASETS[@]} -eq 0 ]] && die "Specify a parent pool/dataset with --all"
            for parent in "${DATASETS[@]}"; do
                while IFS= read -r ds; do
                    backup_dataset "$ds"
                done < <(zfs list -r -o name -H "$parent")
            done
        else
            [[ ${#DATASETS[@]} -eq 0 ]] && die "Specify at least one dataset"
            for ds in "${DATASETS[@]}"; do
                backup_dataset "$ds"
            done
        fi
        ;;
esac
