
#!/usr/bin/env bash

# ==============================================================================
# Customer Churn Analytics - Kafka Topic Initialization
#
# Creates:
#   1. churn.raw   - Raw customer records (7 days retention)
#   2. churn.clean - Clean customer records (7 days retention)
#   3. churn.dlq   - Invalid records (unlimited retention)
#
# Configuration:
#   Partitions:         3
#   Replication Factor: 1 (single Kafka broker)
#
# Usage:
#   bash scripts/create_topics.sh
#
# Run from the project root directory.
# ==============================================================================

set -Eeuo pipefail

# ------------------------------------------------------------------------------
# 1. Configuration
# ------------------------------------------------------------------------------

KAFKA_SERVICE="kafka"
KAFKA_BOOTSTRAP_SERVER="kafka:9092"
KAFKA_TOPICS_COMMAND="/opt/kafka/bin/kafka-topics.sh"

PARTITIONS=3
REPLICATION_FACTOR=1

SEVEN_DAYS_MS=604800000
UNLIMITED_RETENTION=-1

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
# 3. Check prerequisites
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
        error "Please run this script from the project root."
        exit 1
    fi

    success "Prerequisites check passed."
}

# ------------------------------------------------------------------------------
# 4. Wait until Kafka is ready
# ------------------------------------------------------------------------------

wait_for_kafka() {

    log "Waiting for Kafka broker to become available..."

    for ((attempt = 1; attempt <= MAX_RETRIES; attempt++)); do

        if docker compose exec -T "$KAFKA_SERVICE" \
            "$KAFKA_TOPICS_COMMAND" \
            --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
            --list >/dev/null 2>&1; then

            success "Kafka broker is ready."
            return 0
        fi

        log "Kafka is not ready yet ($attempt/$MAX_RETRIES)."

        sleep "$RETRY_INTERVAL"

    done

    error "Kafka did not become ready within the expected time."
    error "Check Kafka logs using: docker compose logs kafka"

    return 1
}

# ------------------------------------------------------------------------------
# 5. Create a topic and configure its retention
# ------------------------------------------------------------------------------

create_topic() {

    local topic_name="$1"
    local retention="$2"

    log "Creating topic: $topic_name"

    docker compose exec -T "$KAFKA_SERVICE" \
        "$KAFKA_TOPICS_COMMAND" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --create \
        --if-not-exists \
        --topic "$topic_name" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR"

    log "Configuring retention for $topic_name..."

    docker compose exec -T "$KAFKA_SERVICE" \
        "$KAFKA_TOPICS_COMMAND" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --alter \
        --topic "$topic_name" \
        --config "retention.ms=$retention"

    success "Topic $topic_name is configured."
}

# ------------------------------------------------------------------------------
# 6. Verify topic configuration
# ------------------------------------------------------------------------------


verify_topic() {

    local topic_name="$1"
    local expected_retention="$2"

    log "Verifying topic: $topic_name"

    local description

    description=$(docker compose exec -T "$KAFKA_SERVICE" \
        "$KAFKA_TOPICS_COMMAND" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --describe \
        --topic "$topic_name")

    echo "$description"

    if ! grep -q "PartitionCount:$PARTITIONS" <<< "$description"; then
        error "$topic_name does not have $PARTITIONS partitions."
        return 1
    fi

    local topic_config

    topic_config=$(docker compose exec -T "$KAFKA_SERVICE" \
        /opt/kafka/bin/kafka-configs.sh \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --entity-type topics \
        --entity-name "$topic_name" \
        --describe)

    echo "$topic_config"

    if ! grep -Fq "retention.ms=$expected_retention" <<< "$topic_config"; then
        error "Incorrect retention configuration for $topic_name."
        return 1
    fi

    success "$topic_name verification passed."
}


# ------------------------------------------------------------------------------
# 7. Main execution
# ------------------------------------------------------------------------------

main() {

    echo
    echo "======================================================"
    echo "     Kafka Topics Initialization"
    echo "======================================================"
    echo

    check_prerequisites

    wait_for_kafka

    echo
    log "Creating project topics..."
    echo

    create_topic "churn.raw" "$SEVEN_DAYS_MS"

    create_topic "churn.clean" "$SEVEN_DAYS_MS"

    create_topic "churn.dlq" "$UNLIMITED_RETENTION"

    echo
    log "Verifying all topic configurations..."
    echo

    verify_topic "churn.raw" "$SEVEN_DAYS_MS"

    verify_topic "churn.clean" "$SEVEN_DAYS_MS"

    verify_topic "churn.dlq" "$UNLIMITED_RETENTION"

    echo
    echo "======================================================"
    success "All Kafka topics created and verified!"
    echo "======================================================"
    echo

    log "Available topics:"

    docker compose exec -T "$KAFKA_SERVICE" \
        "$KAFKA_TOPICS_COMMAND" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --list

}

main "$@"
