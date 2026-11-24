
#!/usr/bin/env bash

# ==============================================================================
# Customer Churn Analytics - Full Environment Reset
#
# WARNING:
# This script permanently deletes the Docker volumes used by this project.
#
# Data affected:
#   - Kafka messages and topics
#   - MySQL database and tables
#   - Spark checkpoints and Ivy cache
#   - NiFi state and configuration
#   - Grafana dashboards and persistent data
#
# Usage:
#   bash scripts/reset_environment.sh
#
# Run from any directory.
# ==============================================================================

set -Eeuo pipefail

# ------------------------------------------------------------------------------
# 1. Project paths
# ------------------------------------------------------------------------------

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

# ------------------------------------------------------------------------------
# 2. Expected Docker volumes
# ------------------------------------------------------------------------------

PROJECT_VOLUMES=(
    "churn_kafka_data"
    "churn_mysql_data"
    "churn_grafana_data"
    "churn_nifi_state"
    "churn_nifi_conf"
    "churn_spark_checkpoints"
    "churn_spark_ivy"
)

# ------------------------------------------------------------------------------
# 3. Logging
# ------------------------------------------------------------------------------

log() {
    echo "[INFO] $1"
}

success() {
    echo "[SUCCESS] $1"
}

warning() {
    echo "[WARNING] $1"
}

error() {
    echo "[ERROR] $1" >&2
}

# ------------------------------------------------------------------------------
# 4. Check prerequisites
# ------------------------------------------------------------------------------

check_prerequisites() {

    log "Checking prerequisites..."

    if ! command -v docker >/dev/null 2>&1; then
        error "Docker is not installed or not available in PATH."
        return 1
    fi

    if ! docker compose version >/dev/null 2>&1; then
        error "Docker Compose is not available."
        return 1
    fi

    if ! docker info >/dev/null 2>&1; then
        error "Docker Engine is not running or is inaccessible."
        return 1
    fi

    if [[ ! -f "$PROJECT_ROOT/docker-compose.yml" ]]; then
        error "docker-compose.yml was not found."
        return 1
    fi

    if [[ ! -f "$PROJECT_ROOT/.env" ]]; then
        error ".env file was not found."
        error "Create it using: cp .env.example .env"
        return 1
    fi

    success "Prerequisites check passed."
}

# ------------------------------------------------------------------------------
# 5. Validate Docker Compose configuration
# ------------------------------------------------------------------------------

validate_compose() {

    log "Validating Docker Compose configuration..."

    cd "$PROJECT_ROOT"

    if ! docker compose config --quiet; then
        error "Docker Compose configuration is invalid."
        return 1
    fi

    success "Docker Compose configuration is valid."
}

# ------------------------------------------------------------------------------
# 6. Display destructive operation warning
# ------------------------------------------------------------------------------

show_reset_warning() {

    echo
    echo "======================================================"
    echo "              ENVIRONMENT RESET WARNING"
    echo "======================================================"
    echo

    warning "THIS OPERATION WILL PERMANENTLY DELETE PROJECT DATA."

    echo
    echo "The following Docker volumes will be removed:"

    for volume in "${PROJECT_VOLUMES[@]}"; do
        echo "  - $volume"
    done

    echo
    echo "This means:"
    echo "  - All Kafka topics and messages will be lost."
    echo "  - MySQL databases and tables will be deleted."
    echo "  - Spark checkpoints and cached dependencies will be deleted."
    echo "  - NiFi state and saved configuration will be deleted."
    echo "  - Grafana persistent data will be deleted."
    echo
    echo "Containers and the Compose network will also be removed."
    echo
    echo "Your project files, source code, .env and SQL files"
    echo "will NOT be deleted by this command."
    echo
    echo "======================================================"
    echo
}

# ------------------------------------------------------------------------------
# 7. Confirm reset
# ------------------------------------------------------------------------------

confirm_reset() {

    local confirmation

    read -r -p "Type RESET to permanently delete the environment: " confirmation

    if [[ "$confirmation" != "RESET" ]]; then
        warning "Reset cancelled. No Docker resources were removed."
        exit 0
    fi

    success "Reset confirmed."
}

# ------------------------------------------------------------------------------
# 8. Stop containers and remove project volumes
# ------------------------------------------------------------------------------

perform_reset() {

    log "Stopping containers and removing project volumes..."

    cd "$PROJECT_ROOT"

    docker compose down --volumes --remove-orphans

    success "Docker Compose environment stopped and removed."
}

# ------------------------------------------------------------------------------
# 9. Verify volume removal
# ------------------------------------------------------------------------------

verify_reset() {

    log "Verifying project volume removal..."

    local remaining=0

    for volume in "${PROJECT_VOLUMES[@]}"; do

        if docker volume inspect "$volume" >/dev/null 2>&1; then
            warning "Volume still exists: $volume"
            remaining=$((remaining + 1))
        else
            success "Removed: $volume"
        fi

    done

    if [[ "$remaining" -gt 0 ]]; then
        error "$remaining project volume(s) still exist."
        error "Check whether another container is using them."
        return 1
    fi

    success "All expected project volumes have been removed."
}

# ------------------------------------------------------------------------------
# 10. Main execution
# ------------------------------------------------------------------------------

main() {

    echo
    echo "======================================================"
    echo "       Customer Churn - Environment Reset"
    echo "======================================================"
    echo

    check_prerequisites

    validate_compose

    show_reset_warning

    confirm_reset

    perform_reset

    verify_reset

    echo
    echo "======================================================"
    success "Environment reset completed successfully!"
    echo "======================================================"
    echo

    log "To start a fresh environment, run:"
    echo "docker compose up -d --build"

    echo
}

main "$@"
