#!/bin/bash
# Downloads and sets up embedded PostgreSQL for integration testing
# Uses pre-built binaries from zonkyio/embedded-postgres-binaries

set -euo pipefail

PG_VERSION="18.1.0"
CACHE_DIR="${CACHE_DIR:-$HOME/.cache/level-pivot-test}"
MAVEN_BASE="https://repo1.maven.org/maven2/io/zonky/test/postgres"

# Global state for cleanup
PG_DIR=""
DATA_DIR=""
PG_PORT=""
PG_PID=""

detect_platform() {
    local os arch
    case "$(uname -s)" in
        Linux)  os="linux" ;;
        Darwin) os="darwin" ;;
        *)      echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
        x86_64)        arch="amd64" ;;
        aarch64|arm64) arch="arm64v8" ;;
        *)             echo "Unsupported arch: $(uname -m)" >&2; exit 1 ;;
    esac
    echo "${os}-${arch}"
}

download_postgres() {
    local platform="$1"
    local jar_name="embedded-postgres-binaries-${platform}-${PG_VERSION}.jar"
    local jar_path="${CACHE_DIR}/${jar_name}"
    local pg_dir="${CACHE_DIR}/postgres-${PG_VERSION}-${platform}"

    if [[ -d "${pg_dir}/bin" ]]; then
        echo "Using cached PostgreSQL at ${pg_dir}" >&2
        echo "${pg_dir}"
        return 0
    fi

    mkdir -p "${CACHE_DIR}"
    local url="${MAVEN_BASE}/embedded-postgres-binaries-${platform}/${PG_VERSION}/${jar_name}"

    echo "Downloading PostgreSQL ${PG_VERSION} for ${platform}..." >&2
    if ! curl -sL "${url}" -o "${jar_path}"; then
        echo "Failed to download PostgreSQL binaries" >&2
        exit 1
    fi

    echo "Extracting..." >&2
    local tmp_dir
    tmp_dir=$(mktemp -d)

    # JAR is a ZIP containing a .txz file
    unzip -q -j "${jar_path}" '*.txz' -d "${tmp_dir}"

    # Extract PostgreSQL from .txz (tar + xz)
    mkdir -p "${pg_dir}"
    tar -xJf "${tmp_dir}"/*.txz -C "${pg_dir}"

    # Cleanup
    rm -rf "${tmp_dir}" "${jar_path}"

    echo "${pg_dir}"
}

init_database() {
    local pg_dir="$1"
    local data_dir="$2"

    echo "Initializing database at ${data_dir}..." >&2

    # Set library path for initdb
    if [[ "$(uname -s)" == "Darwin" ]]; then
        export DYLD_LIBRARY_PATH="${pg_dir}/lib:${DYLD_LIBRARY_PATH:-}"
    else
        export LD_LIBRARY_PATH="${pg_dir}/lib:${LD_LIBRARY_PATH:-}"
    fi

    "${pg_dir}/bin/initdb" \
        -D "${data_dir}" \
        -U postgres \
        --no-locale \
        -E UTF8 \
        >/dev/null 2>&1
}

find_free_port() {
    # Find a free port in the 15432-25432 range
    local port
    for port in $(shuf -i 15432-25432 -n 100); do
        if ! nc -z localhost "$port" 2>/dev/null; then
            echo "$port"
            return 0
        fi
    done
    echo "Could not find free port" >&2
    exit 1
}

wait_for_postgres() {
    local pg_dir="$1"
    local data_dir="$2"
    local port="$3"

    # Wait for PostgreSQL to be ready by checking if we can connect
    local retries=30
    while [[ $retries -gt 0 ]]; do
        # Try to connect using postgres single-user mode query or check socket
        if [[ -S "${data_dir}/.s.PGSQL.${port}" ]]; then
            # Socket exists, try a simple connection using psql if available
            if command -v psql >/dev/null 2>&1; then
                if psql -h "${data_dir}" -p "${port}" -U postgres -c "SELECT 1" postgres >/dev/null 2>&1; then
                    return 0
                fi
            else
                # No psql, just check that the socket exists and postmaster.pid is stable
                if [[ -f "${data_dir}/postmaster.pid" ]]; then
                    sleep 0.5
                    return 0
                fi
            fi
        fi
        retries=$((retries - 1))
        sleep 0.5
    done
    return 1
}

start_postgres() {
    local pg_dir="$1"
    local data_dir="$2"
    local port="$3"

    echo "Starting PostgreSQL on port ${port}..." >&2

    # Set library path
    if [[ "$(uname -s)" == "Darwin" ]]; then
        export DYLD_LIBRARY_PATH="${pg_dir}/lib:${DYLD_LIBRARY_PATH:-}"
    else
        export LD_LIBRARY_PATH="${pg_dir}/lib:${LD_LIBRARY_PATH:-}"
    fi

    # Start PostgreSQL
    "${pg_dir}/bin/pg_ctl" \
        -D "${data_dir}" \
        -l "${data_dir}/postgres.log" \
        -o "-p ${port} -k ${data_dir}" \
        start >/dev/null 2>&1

    # Wait for PostgreSQL to be ready
    if ! wait_for_postgres "${pg_dir}" "${data_dir}" "${port}"; then
        echo "PostgreSQL failed to start. Log:" >&2
        cat "${data_dir}/postgres.log" >&2
        exit 1
    fi

    echo "PostgreSQL started successfully" >&2
}

stop_postgres() {
    local pg_dir="$1"
    local data_dir="$2"

    if [[ -n "${pg_dir}" && -n "${data_dir}" && -f "${data_dir}/postmaster.pid" ]]; then
        echo "Stopping PostgreSQL..." >&2
        "${pg_dir}/bin/pg_ctl" -D "${data_dir}" stop -m fast >/dev/null 2>&1 || true
    fi
}

create_test_database() {
    local pg_dir="$1"
    local data_dir="$2"
    local port="$3"
    local db_name="${4:-test_db}"

    echo "Creating database ${db_name}..." >&2
    # Use system psql if available, connecting via Unix socket
    if command -v psql >/dev/null 2>&1; then
        psql -h "${data_dir}" -p "${port}" -U postgres -c "CREATE DATABASE ${db_name}" postgres 2>/dev/null || true
    else
        echo "Warning: psql not found, skipping database creation (will use postgres db)" >&2
    fi
}

install_extension() {
    local pg_dir="$1"
    local build_dir="$2"
    local sql_dir="$3"

    echo "Installing level_pivot extension..." >&2

    # Copy shared library to pkglibdir (lib/postgresql/)
    # This is where PostgreSQL's $libdir points to for extensions
    local lib_dir="${pg_dir}/lib/postgresql"
    local lib_name="level_pivot.so"

    if [[ -f "${build_dir}/${lib_name}" ]]; then
        cp "${build_dir}/${lib_name}" "${lib_dir}/"
    elif [[ -f "${build_dir}/Release/${lib_name}" ]]; then
        cp "${build_dir}/Release/${lib_name}" "${lib_dir}/"
    elif [[ -f "${build_dir}/release/${lib_name}" ]]; then
        cp "${build_dir}/release/${lib_name}" "${lib_dir}/"
    else
        echo "Could not find ${lib_name} in ${build_dir}" >&2
        exit 1
    fi

    # Copy SQL and control files
    # Embedded PostgreSQL uses share/postgresql/extension/ path
    local ext_dir="${pg_dir}/share/postgresql/extension"
    mkdir -p "${ext_dir}"
    cp "${sql_dir}/level_pivot.control" "${ext_dir}/"
    cp "${sql_dir}/level_pivot--1.0.sql" "${ext_dir}/"
}

setup_embedded_postgres() {
    local platform
    platform=$(detect_platform)

    PG_DIR=$(download_postgres "${platform}")
    DATA_DIR=$(mktemp -d)
    PG_PORT=$(find_free_port)

    init_database "${PG_DIR}" "${DATA_DIR}"
    start_postgres "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}"
    create_test_database "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "test_db"

    # Export for use by caller
    export PG_DIR DATA_DIR PG_PORT
}

cleanup() {
    if [[ -n "${PG_DIR:-}" && -n "${DATA_DIR:-}" ]]; then
        stop_postgres "${PG_DIR}" "${DATA_DIR}"
    fi
    if [[ -n "${DATA_DIR:-}" && -d "${DATA_DIR:-}" ]]; then
        rm -rf "${DATA_DIR}"
    fi
    # Clean up test LevelDB directory
    rm -rf /tmp/level_pivot_test
}

# Helper to run psql commands
# Uses system psql since embedded binaries are minimal
run_psql() {
    local pg_dir="$1"
    local data_dir="$2"
    local port="$3"
    local db="${4:-test_db}"
    shift 4

    if ! command -v psql >/dev/null 2>&1; then
        echo "Error: psql not found. Please install PostgreSQL client tools." >&2
        exit 1
    fi

    psql \
        -h "${data_dir}" \
        -p "${port}" \
        -U postgres \
        -d "${db}" \
        --set ON_ERROR_STOP=1 \
        "$@"
}
