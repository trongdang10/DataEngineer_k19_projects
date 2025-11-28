#!/usr/bin/env bash
# Restore MongoDB data from GCS on the VM.
# - Downloads the Glamira dump from GCS
# - Extracts the archive
# - Restores into MongoDB
# Usage: bash scripts/5_mongodb_restore.sh (override config via env vars if needed)
set -euo pipefail

# -------- Config (adjust if needed) --------
GCS_URI=${GCS_URI:-"gs://raw_glamira_data/glamira_ubl_oct2019_nov2019.tar.gz"}
LOCAL_TAR=${LOCAL_TAR:-"$HOME/glamira_ubl_oct2019_nov2019.tar.gz"}
EXTRACT_DIR=${EXTRACT_DIR:-"$HOME/glamira_dump"}
MONGO_URI=${MONGO_URI:-"mongodb://localhost:27017"}
MONGO_DB=${MONGO_DB:-"glamira"}
RESTORE_SUBDIR=${RESTORE_SUBDIR:-"$EXTRACT_DIR/dump/countly"}
# -------------------------------------------

log() { printf '[%s] %s\n' "$(date +'%F %T')" "$*"; }

download() {
  # Copy the dump from GCS to a local tar file
  log "Downloading $GCS_URI to $LOCAL_TAR"
  if command -v gsutil >/dev/null 2>&1; then
    gsutil cp "$GCS_URI" "$LOCAL_TAR"
  elif command -v gcloud >/dev/null 2>&1; then
    gcloud storage cp "$GCS_URI" "$LOCAL_TAR"
  else
    log "ERROR: neither gsutil nor gcloud is available."
    exit 1
  fi
}

extract() {
  # Extract the downloaded tarball into the working directory
  log "Extracting $LOCAL_TAR into $EXTRACT_DIR"
  mkdir -p "$EXTRACT_DIR"
  tar -xf "$LOCAL_TAR" -C "$EXTRACT_DIR"
}

restore() {
  # Restore the extracted Mongo dump into the target database
  if [[ ! -d "$RESTORE_SUBDIR" ]]; then
    log "ERROR: restore directory not found: $RESTORE_SUBDIR"
    exit 1
  fi
  log "Restoring MongoDB from $RESTORE_SUBDIR into $MONGO_URI/$MONGO_DB"
  mongorestore --uri="$MONGO_URI/$MONGO_DB" --drop "$RESTORE_SUBDIR"
}

main() {
  download
  extract
  restore
  log "Restore complete."
}

main "$@"
