
#!/usr/bin/env bash

# ==============================================================================
# Customer Churn Analytics - Spark Job Submission
#
# Responsibilities:
#   1. Validate Docker and Docker Compose.
#   2. Verify Spark Master availability.
#   3. Verify the Spark application exists inside the container.
#   4. Load Kafka and MySQL JDBC dependencies.
#   5. Submit the Structured Streaming application to Spark Standalone.
#
# Spark:
#   Version: 3.5.1
#   Scala:   2.12
#
# Usage:
#   bash scripts/submit_spark.sh
#
#   With application arguments:
#   bash scripts/submit_spark.sh --some-option value
#
# ==============================================================================

set -Eeuo pipefail

# ------------------------------------------------------------------------------
# 1. Configuration
# ------------------------------------------------------------------------------

SPARK_SERVICE="spark-master"

SPARK_SUBMIT="/opt/spark/bin/spark-submit"

SPARK_MASTER_URL="spark://spark-master:7077"

SPARK_APP="/opt/spark-apps/streaming_job.py"

SPARK_VERSION="3.5.1"
SCALA_VERSION="2.12"

KAFKA_CONNECTOR="org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION}"

MYSQL_CONNECTOR_VERSION="${MYSQL_CONNECTOR_VERSION:-8.4.0}"

MYSQL_CONNECTOR="com.mysql:mysql-connector-j:${MYSQL_CONNECTOR_VERSION}"

IVY_CACHE="/tmp/.ivy2"

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

    error "Spark submission failed."
    error "Exit code: $exit_code"

    error "Check Spark Master logs:"
    error "docker compose logs --tail 50 spark-master"

    error "Check Spark Worker logs:"
    error "docker compose logs --tail 50 spark-worker"

    exit "$exit_code"
}

trap on_error ERR

# ------------------------------------------------------------------------------
# 4. Prerequisites
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

    if [[ ! -f "docker-compose.yml" ]]; then
        error "docker-compose.yml not found."
        error "Run this script from the project root."
        exit 1
    fi

    success "Prerequisites check passed."
}

# ------------------------------------------------------------------------------
# 5. Verify Spark Master
# ------------------------------------------------------------------------------

check_spark_master() {

    log "Checking Spark Master availability..."

    if ! docker compose ps --status running --services |
        grep -Fxq "$SPARK_SERVICE"; then

        error "Spark Master is not running."
        error "Start it using: docker compose up -d spark-master spark-worker"
        return 1
    fi

    if ! docker compose exec -T "$SPARK_SERVICE" \
        "$SPARK_SUBMIT" --version >/dev/null 2>&1; then

        error "Spark is not available inside the Spark Master container."
        return 1
    fi

    success "Spark Master is running."
}

# ------------------------------------------------------------------------------
# 6. Verify application file
# ------------------------------------------------------------------------------

check_spark_application() {

    log "Checking Spark application..."

    if ! docker compose exec -T "$SPARK_SERVICE" \
        test -f "$SPARK_APP"; then

        error "Spark application was not found:"
        error "$SPARK_APP"

        error "Expected source file:"
        error "spark/streaming_job.py"

        error "Check the Spark volume mapping in docker-compose.yml."
        error "The project source folder must be mounted at /opt/spark-apps."

        return 1
    fi

    success "Spark application found."
}

# ------------------------------------------------------------------------------
# 7. Submit Spark job
# ------------------------------------------------------------------------------

submit_job() {

    log "Preparing Spark dependencies..."

    log "Kafka Connector: $KAFKA_CONNECTOR"
    log "MySQL Connector: $MYSQL_CONNECTOR"

    log "Submitting Spark Structured Streaming job..."

    docker compose exec -T "$SPARK_SERVICE" \
        "$SPARK_SUBMIT" \
        --master "$SPARK_MASTER_URL" \
        --deploy-mode client \
        --packages "$KAFKA_CONNECTOR,$MYSQL_CONNECTOR" \
        --conf "spark.jars.ivy=$IVY_CACHE" \
        "$SPARK_APP" \
        "$@"

    success "Spark job completed successfully."
}

# ------------------------------------------------------------------------------
# 8. Main execution
# ------------------------------------------------------------------------------

main() {

    echo
    echo "======================================================"
    echo "       Customer Churn - Spark Job Submission"
    echo "======================================================"
    echo

    check_prerequisites

    check_spark_master

    check_spark_application

    submit_job "$@"

    echo
    echo "======================================================"
    success "Spark submission process completed!"
    echo "======================================================"
    echo

}

main "$@"
