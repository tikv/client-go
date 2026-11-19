#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
compose_file="${script_dir}/docker-compose.yml"

if ! compose_version=$(docker compose version --short 2>/dev/null); then
    echo "Docker Compose v2 or newer is required" >&2
    exit 1
fi
normalized_compose_version=${compose_version#v}
compose_major=${normalized_compose_version%%.*}
case "${compose_major}" in
    '' | *[!0-9]*)
        echo "Docker Compose v2 or newer is required (found: ${compose_version})" >&2
        exit 1
        ;;
esac
if [ "${compose_major}" -lt 2 ]; then
    echo "Docker Compose v2 or newer is required (found: ${compose_version})" >&2
    exit 1
fi

if [ -n "${COMPOSE_PROJECT_NAME:-}" ]; then
    project=${COMPOSE_PROJECT_NAME}
else
    user_name=${USER:-user}
    sanitized_user=$(printf '%s' "${user_name}" | tr '[:upper:]' '[:lower:]' | tr -c 'abcdefghijklmnopqrstuvwxyz0123456789_-' '-')
    if [ -z "${sanitized_user}" ]; then
        sanitized_user=user
    fi
    random_suffix=$(od -An -N4 -tu4 /dev/urandom | tr -d '[:space:]')
    project="client-go-txn-file-${sanitized_user}-$(date +%Y%m%d%H%M%S)-${random_suffix}"
fi

compose() {
    docker compose --project-name "${project}" --file "${compose_file}" "$@"
}

wait_healthy() {
    service=$1
    attempt=1
    while [ "${attempt}" -le 180 ]; do
        container=$(compose ps --all --quiet "${service}" 2>/dev/null || true)
        if [ -n "${container}" ]; then
            state=$(docker inspect --format '{{.State.Status}}' "${container}" 2>/dev/null || true)
            exit_code=$(docker inspect --format '{{.State.ExitCode}}' "${container}" 2>/dev/null || true)
            health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${container}" 2>/dev/null || true)
            case "${state}" in
                exited)
                    echo "${service} exited with exit code ${exit_code} while waiting to become healthy (health: ${health})" >&2
                    return 1
                    ;;
                dead)
                    echo "${service} entered a dead state with exit code ${exit_code} while waiting to become healthy (health: ${health})" >&2
                    return 1
                    ;;
            esac
            case "${health}" in
                healthy)
                    return 0
                    ;;
                unhealthy)
                    echo "${service} became unhealthy" >&2
                    return 1
                    ;;
            esac
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    echo "timed out waiting for ${service} to become healthy" >&2
    return 1
}

wait_completed() {
    service=$1
    attempt=1
    while [ "${attempt}" -le 180 ]; do
        container=$(compose ps --all --quiet "${service}" 2>/dev/null || true)
        if [ -n "${container}" ]; then
            state=$(docker inspect --format '{{.State.Status}}:{{.State.ExitCode}}' "${container}" 2>/dev/null || true)
            case "${state}" in
                exited:0)
                    return 0
                    ;;
                exited:*)
                    echo "${service} exited with ${state#exited:}" >&2
                    return 1
                    ;;
                dead:*)
                    echo "${service} entered a dead state" >&2
                    return 1
                    ;;
            esac
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    echo "timed out waiting for ${service} to complete" >&2
    return 1
}

print_file_log() {
    service=$1
    log_path=$2

    printf '\n===== %s: %s =====\n' "${service}" "${log_path}" >&2
    if compose exec -T "${service}" sh -c 'cat "$1"' sh "${log_path}"; then
        return 0
    fi

    container=$(compose ps --all --quiet "${service}" 2>/dev/null || true)
    if [ -z "${container}" ]; then
        echo "no container found to collect ${log_path} from ${service}" >&2
        return 0
    fi
    if ! command -v tar >/dev/null 2>&1; then
        echo "tar is unavailable; cannot collect ${log_path} from stopped ${service} container ${container}" >&2
        return 0
    fi
    if ! docker cp "${container}:${log_path}" - | tar -xOf -; then
        echo "could not collect ${log_path} from stopped ${service} container ${container}" >&2
    fi
    return 0
}

diagnostics() {
    echo "collecting Compose diagnostics for ${project}" >&2
    compose ps || true
    compose logs --no-color || true
    print_file_log pd /var/log/pd/pd.log
    print_file_log tikv-1 /var/log/tikv/tikv.log
    print_file_log tikv-2 /var/log/tikv/tikv.log
    print_file_log tikv-3 /var/log/tikv/tikv.log
    print_file_log tikv-worker /var/log/tikv-worker/tikv-worker.log
    print_file_log test /workspace/integration_tests_in_docker/txn_file/txn-file.log
}

cleanup() {
    status=$?
    trap - 0
    set +e
    if [ "${status}" -ne 0 ]; then
        diagnostics
    fi
    compose down -v --remove-orphans --rmi local
    down_status=$?
    if [ "${status}" -eq 0 ] && [ "${down_status}" -ne 0 ]; then
        status=${down_status}
    fi
    exit "${status}"
}

compose config --quiet
trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

compose up -d pd minio
wait_healthy pd
wait_healthy minio

compose up -d --no-deps minio-init
wait_completed minio-init

compose up -d tikv-1 tikv-2 tikv-3
wait_healthy tikv-1
wait_healthy tikv-2
wait_healthy tikv-3

compose up -d --no-deps create-keyspace
wait_completed create-keyspace

compose up -d --no-deps tikv-worker
wait_healthy tikv-worker

compose build test
compose run --no-deps test
