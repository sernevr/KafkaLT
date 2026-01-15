#!/bin/bash

# Kafka Latency Test Runner
# This script checks broker connectivity before running producer/consumer latency tests
# Features: Parallel consumption, Low-latency client configuration, MSK optimized

# Default configuration
DEFAULT_COUNT=1000
DEFAULT_TOPIC="clickstream-latency-test"
DEFAULT_TIMEOUT=10
DEFAULT_WAIT_AFTER_PRODUCER=10
DEFAULT_SASL_MECHANISM="PLAIN"
DEFAULT_SECRET_NAME="AmazonMSK_Manager"

# Initialize with defaults
BROKERS=""
COUNT="$DEFAULT_COUNT"
TOPIC="$DEFAULT_TOPIC"
TIMEOUT="$DEFAULT_TIMEOUT"
WAIT_AFTER="$DEFAULT_WAIT_AFTER_PRODUCER"
SECRET_NAME="$DEFAULT_SECRET_NAME"
SASL_MECHANISM="$DEFAULT_SASL_MECHANISM"
TLS_ENABLED=false
TLS_SKIP_VERIFY=false
USE_SCRAM=false

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="$SCRIPT_DIR/out/kafkalt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -b, --brokers BROKERS    Comma-separated list of Kafka brokers (required)"
    echo "                           Format: hostname:port or host1:port1,host2:port2"
    echo "  -c, --count COUNT        Number of messages to produce (default: 1000)"
    echo "  -t, --topic TOPIC        Kafka topic name (default: clickstream-latency-test)"
    echo "  -T, --timeout SECONDS    Connection timeout per broker (default: 10)"
    echo "  -w, --wait SECONDS       Wait time after producer finishes (default: 10)"
    echo "  -S, --secret NAME        AWS Secrets Manager secret name for SCRAM auth"
    echo "                           (default: AmazonMSK_Manager)"
    echo "  --scram                  Enable SCRAM authentication using AWS Secrets Manager"
    echo "  -m, --mechanism MECH     SASL mechanism: SCRAM-SHA-256, SCRAM-SHA-512"
    echo "                           (default: PLAIN, used with --scram)"
    echo "  -s, --tls                Enable TLS encryption (optional)"
    echo "  -k, --tls-skip-verify    Skip TLS certificate verification (optional)"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -b localhost:9092"
    echo "  $0 -b localhost:9092 -c 100"
    echo "  $0 -b broker1:9092,broker2:9092 -c 500 -t my-topic"
    echo "  $0 --brokers localhost:9092 --count 100 --topic test-topic"
    echo "  $0 --scram -m SCRAM-SHA-512    # SCRAM auth with default secret"
    echo "  $0 --scram -S MyCustomSecret   # SCRAM auth with custom secret"
    echo "  $0 -s -k                       # TLS with skip verify"
    echo "  $0 -T 30 -w 20"
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--brokers)
            BROKERS="$2"
            shift 2
            ;;
        -c|--count)
            COUNT="$2"
            shift 2
            ;;
        -t|--topic)
            TOPIC="$2"
            shift 2
            ;;
        -T|--timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_AFTER="$2"
            shift 2
            ;;
        -S|--secret)
            SECRET_NAME="$2"
            shift 2
            ;;
        --scram)
            USE_SCRAM=true
            shift
            ;;
        -m|--mechanism)
            SASL_MECHANISM="$2"
            shift 2
            ;;
        -s|--tls)
            TLS_ENABLED=true
            shift
            ;;
        -k|--tls-skip-verify)
            TLS_SKIP_VERIFY=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}ERROR: Unknown option: $1${NC}"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Check if brokers is provided
if [[ -z "$BROKERS" ]]; then
    echo -e "${RED}ERROR: Brokers is required${NC}"
    echo "Use -b or --brokers to specify Kafka brokers"
    echo "Example: $0 -b hostname:port"
    echo "         $0 -b host1:port1,host2:port2"
    exit 1
fi

# Check if binary exists
if [[ ! -x "$BINARY" ]]; then
    echo -e "${RED}ERROR: kafka-latency-test binary not found at $BINARY${NC}"
    echo "Please build it first: go build -o out/kafkalt main.go"
    exit 1
fi

# Fetch credentials from AWS Secrets Manager if SCRAM is enabled
USERNAME=""
PASSWORD=""
if [[ "$USE_SCRAM" == true ]]; then
    echo -e "${YELLOW}Fetching credentials from AWS Secrets Manager...${NC}"
    echo "Secret Name: $SECRET_NAME"

    # Check if AWS CLI is available
    if ! command -v aws &> /dev/null; then
        echo -e "${RED}ERROR: AWS CLI is not installed or not in PATH${NC}"
        exit 1
    fi

    # Fetch secret from AWS Secrets Manager
    SECRET_VALUE=$(aws secretsmanager get-secret-value --secret-id "$SECRET_NAME" --query 'SecretString' --output text 2>&1)
    if [[ $? -ne 0 ]]; then
        echo -e "${RED}ERROR: Failed to fetch secret from AWS Secrets Manager${NC}"
        echo "$SECRET_VALUE"
        exit 1
    fi

    # Parse username and password from JSON secret
    USERNAME=$(echo "$SECRET_VALUE" | jq -r '.username // empty')
    PASSWORD=$(echo "$SECRET_VALUE" | jq -r '.password // empty')

    if [[ -z "$USERNAME" || -z "$PASSWORD" ]]; then
        echo -e "${RED}ERROR: Secret does not contain 'username' or 'password' fields${NC}"
        exit 1
    fi

    echo -e "${GREEN}Credentials fetched successfully${NC}"
    echo ""
fi

# Build extra arguments for auth and TLS
EXTRA_ARGS=""
if [[ -n "$USERNAME" && -n "$PASSWORD" ]]; then
    EXTRA_ARGS="-username=$USERNAME -password=$PASSWORD -sasl-mechanism=$SASL_MECHANISM"
fi
if [[ "$TLS_ENABLED" == true ]]; then
    EXTRA_ARGS="$EXTRA_ARGS -tls"
fi
if [[ "$TLS_SKIP_VERIFY" == true ]]; then
    EXTRA_ARGS="$EXTRA_ARGS -tls-skip-verify"
fi

echo "============================================"
echo "   KAFKA LATENCY TEST RUNNER (v2.1-lowlat)"
echo "============================================"
echo "Brokers: $BROKERS"
echo "Message Count: $COUNT"
echo "Topic: $TOPIC"
echo "Connection Timeout: ${TIMEOUT}s per broker"
echo "Wait After Producer: ${WAIT_AFTER}s"
if [[ "$TLS_ENABLED" == true ]]; then
    echo "TLS: Enabled (Skip Verify: $TLS_SKIP_VERIFY)"
else
    echo "TLS: Disabled"
fi
if [[ "$USE_SCRAM" == true ]]; then
    echo "Authentication: SASL/SCRAM ($SASL_MECHANISM)"
    echo "Secret: $SECRET_NAME"
    echo "Username: $USERNAME"
else
    echo "Authentication: None"
fi
echo "============================================"
echo ""

# Extract broker hosts and ports for connectivity check
echo -e "${YELLOW}=== BROKER CONNECTIVITY CHECK ===${NC}"

IFS=',' read -ra BROKER_ARRAY <<< "$BROKERS"
for broker in "${BROKER_ARRAY[@]}"; do
    # Extract host and port
    host="${broker%:*}"
    port="${broker##*:}"

    # Default port if not specified
    if [[ "$host" == "$port" ]]; then
        port="9092"
    fi

    echo -n "Checking $host:$port... "

    if nc -z -w "$TIMEOUT" "$host" "$port" 2>/dev/null; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
        echo ""
        echo -e "${RED}ERROR: Could not connect to broker $host:$port${NC}"
        echo "Please check:"
        echo "  - Broker is running"
        echo "  - Network connectivity"
        echo "  - Firewall rules"
        echo "  - VPN connection (if required)"
        exit 1
    fi
done

echo ""
echo -e "${GREEN}=== ALL BROKERS REACHABLE ===${NC}"
echo ""

# Start consumer in background
echo -e "${YELLOW}Starting consumer in background...${NC}"
if [[ -n "$EXTRA_ARGS" ]]; then
    "$BINARY" -mode=consumer -brokers="$BROKERS" -topic="$TOPIC" $EXTRA_ARGS &
else
    "$BINARY" -mode=consumer -brokers="$BROKERS" -topic="$TOPIC" &
fi
CONSUMER_PID=$!
echo "Consumer PID: $CONSUMER_PID"
echo ""

# Give consumer a moment to initialize
sleep 2

# Start producer
echo -e "${YELLOW}Starting producer with $COUNT messages...${NC}"
echo ""
if [[ -n "$EXTRA_ARGS" ]]; then
    "$BINARY" -mode=producer -brokers="$BROKERS" -topic="$TOPIC" -count="$COUNT" $EXTRA_ARGS
else
    "$BINARY" -mode=producer -brokers="$BROKERS" -topic="$TOPIC" -count="$COUNT"
fi
PRODUCER_EXIT_CODE=$?

echo ""
if [[ $PRODUCER_EXIT_CODE -eq 0 ]]; then
    echo -e "${GREEN}Producer finished successfully.${NC}"
else
    echo -e "${RED}Producer exited with code: $PRODUCER_EXIT_CODE${NC}"
fi

# Wait for remaining messages to be consumed
echo "Waiting ${WAIT_AFTER} seconds for consumer to process remaining messages..."
sleep "$WAIT_AFTER"

# Stop consumer
echo "Stopping consumer (PID: $CONSUMER_PID)..."
kill "$CONSUMER_PID" 2>/dev/null

# Wait for consumer to exit gracefully
wait "$CONSUMER_PID" 2>/dev/null

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}       TEST COMPLETED${NC}"
echo -e "${GREEN}============================================${NC}"
echo "Check the log/ directory for CSV output files"
echo "Check the sum/ directory for summary files"
