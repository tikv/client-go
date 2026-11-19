#!/bin/sh
set -eu

MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_USER=${MINIO_ROOT_USER:-minioadmin}
MINIO_PASSWORD=${MINIO_ROOT_PASSWORD:-minioadmin}
BUCKET=tidbcloud-local-dfs

if command -v mc >/dev/null 2>&1; then
    mc_bin=$(command -v mc)
elif command -v mcli >/dev/null 2>&1; then
    mc_bin=$(command -v mcli)
else
    echo "mc or mcli is required to initialize MinIO" >&2
    exit 1
fi

"${mc_bin}" alias set local "${MINIO_ENDPOINT}" "${MINIO_USER}" "${MINIO_PASSWORD}"

if command -v curl >/dev/null 2>&1; then
    healthcheck() {
        curl -fsS "${MINIO_ENDPOINT}/minio/health/live" >/dev/null
    }
elif command -v wget >/dev/null 2>&1; then
    healthcheck() {
        wget -q -O - "${MINIO_ENDPOINT}/minio/health/live" >/dev/null
    }
else
    healthcheck() {
        "${mc_bin}" ready local >/dev/null
    }
fi

attempt=1
while [ "${attempt}" -le 120 ]; do
    if healthcheck; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
if [ "${attempt}" -gt 120 ]; then
    echo "MinIO did not become ready at ${MINIO_ENDPOINT}" >&2
    exit 1
fi

"${mc_bin}" mb --ignore-existing "local/${BUCKET}"
"${mc_bin}" stat "local/${BUCKET}"
