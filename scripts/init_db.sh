
#!/usr/bin/env bash

# ==============================================================================
# Customer Churn Analytics - MySQL Database Initialization
#
# Responsibilities:
#   1. Validate Docker and project configuration.
#   2. Wait until MySQL is ready.
#   3. Discover SQL files inside ./sql.
#   4. Execute SQL files in alphabetical order.
#
# Usage:
#   bash scripts/init_db.sh
#
# Requirements:
#   - Run from any directory.
#   - Docker Compose must be installed.
#   - .env must exist in the project root.
#   - SQL files must exist inside ./sql.
# ==============================================================================

set -Eeuo pipefail

# ------------------------------------------------------------------------------
# 1. Project paths and configuration
# ------------------------------------------------------------------------------

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

SQL_DIR="$PROJECT_ROOT/sql"

DB_SERVICE="db"

MAX_RETRIES=30
RETRY_INTERVAL=2

# ------------------------------------------------------------------------------
# 2. Logging
# ------------------------------------------------------------------------------

log() {
    echo "[INFO] $1"
}

success() {
    echo "[SUCCESS] $1"
}

error() {
    echo "[ERROR] $1" >&2
}

# ------------------------------------------------------------------------------
# 3. Error handling
# ------------------------------------------------------------------------------

on_error() {
    local exit_code=$?
    error "Initialization failed at line ${BASH_LINENO[0]}."
    error "Exit code: $exit_code"
    error "Check the error above and review MySQL logs:"
    error "docker compose logs db"
    exit "$exit_code"
}

trap on_error ERR

# ------------------------------------------------------------------------------
# 4. Check prerequisites
# ------------------------------------------------------------------------------

check_prerequisites() {

    log "Checking prerequisites..."

    if ! command -v docker >/dev/null 2>&1; then
        error "Docker is not installed or not available in PATH."
        exit 1
    fi

    if ! docker compose version >/dev/null 2>&1; then
        error "Docker Compose is not available."
        exit 1
    fi

    if [[ ! -f "$PROJECT_ROOT/docker-compose.yml" ]]; then
        error "docker-compose.yml was not found."
        exit 1
    fi

    if [[ ! -f "$PROJECT_ROOT/.env" ]]; then
        error ".env file was not found."
        error "Create it using: cp .env.example .env"
        exit 1
    fi

    if [[ ! -d "$SQL_DIR" ]]; then
        error "SQL directory was not found: $SQL_DIR"
        exit 1
    fi

    success "Prerequisites check passed."
}

# ------------------------------------------------------------------------------
# 5. Discover SQL files
# ------------------------------------------------------------------------------

discover_sql_files() {

    log "Searching for SQL initialization files..."

    mapfile -t SQL_FILES < <(
        find "$SQL_DIR" -maxdepth 1 -type f -name '*.sql' -print | sort
    )

    if [[ ${#SQL_FILES[@]} -eq 0 ]]; then
        error "No SQL files were found inside ./sql."
        error "Add the project's schema files before running initialization."
        return 1
    fi

    success "Found ${#SQL_FILES[@]} SQL file(s)."

    for sql_file in "${SQL_FILES[@]}"; do
        log "  - $(basename "$sql_file")"
    done
}

# ------------------------------------------------------------------------------
# 6. Wait for MySQL
# ------------------------------------------------------------------------------

wait_for_mysql() {

    log "Waiting for MySQL to become ready..."

    for ((attempt = 1; attempt <= MAX_RETRIES; attempt++)); do

        if docker compose exec -T "$DB_SERVICE" \
            sh -c 'mysqladmin ping -h 127.0.0.1 -u root -p"$MYSQL_ROOT_PASSWORD" --silent' \
            >/dev/null 2>&1; then

            success "MySQL is ready."
            return 0
        fi

        log "MySQL is not ready yet ($attempt/$MAX_RETRIES)."

        sleep "$RETRY_INTERVAL"

    done

    error "MySQL did not become ready within 60 seconds."
    error "Check MySQL logs using: docker compose logs db"

    return 1
}

# ------------------------------------------------------------------------------
# 7. Execute SQL files
# ------------------------------------------------------------------------------

execute_sql_file() {

    local sql_file="$1"
    local filename

    filename="$(basename "$sql_file")"

    log "Executing SQL file: $filename"

    docker compose exec -T "$DB_SERVICE" \
        sh -c 'exec mysql --default-character-set=utf8mb4 --show-warnings -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE"' \
        < "$sql_file"

    success "Successfully executed: $filename"
}

# ------------------------------------------------------------------------------
# 8. Execute all SQL files
# ------------------------------------------------------------------------------

initialize_database() {

    log "Starting database initialization..."

    local sql_file

    for sql_file in "${SQL_FILES[@]}"; do
        execute_sql_file "$sql_file"
    done

    success "All SQL files were executed successfully."
}

# ------------------------------------------------------------------------------
# 9. Main execution
# ------------------------------------------------------------------------------

main() {

    echo
    echo "======================================================"
    echo "       MySQL Database Initialization"
    echo "======================================================"
    echo

    cd "$PROJECT_ROOT"

    check_prerequisites

    discover_sql_files

    wait_for_mysql

    initialize_database

    echo
    echo "======================================================"
    success "Database initialization completed!"
    echo "======================================================"
    echo

}

main "$@"
