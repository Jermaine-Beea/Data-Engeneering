#!/bin/bash

# HVS (ScyllaDB) Setup and Stream Processor Launch Script
# This script initializes ScyllaDB and starts the stream processor

set -e  # Exit on error

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}HVS Setup & Stream Processor${NC}"
echo -e "${BLUE}========================================${NC}"

# Configuration
SCYLLA_CONTAINER="scylla"
MAX_WAIT=120  # Maximum wait time in seconds
SCHEMA_FILE="schema.cql"

# Function to check if ScyllaDB is ready
wait_for_scylla() {
    echo -e "${YELLOW}[1/4] Waiting for ScyllaDB to be ready...${NC}"
    
    local elapsed=0
    while [ $elapsed -lt $MAX_WAIT ]; do
        if docker exec $SCYLLA_CONTAINER nodetool status 2>/dev/null | grep -q "UN"; then
            echo -e "${GREEN}✓ ScyllaDB is ready${NC}"
            return 0
        fi
        echo -n "."
        sleep 5
        elapsed=$((elapsed + 5))
    done
    
    echo -e "${RED}✗ ScyllaDB failed to start within ${MAX_WAIT} seconds${NC}"
    exit 1
}

# Function to check if keyspace exists
check_keyspace() {
    echo -e "${YELLOW}[2/4] Checking if keyspace exists...${NC}"
    
    local keyspace_exists=$(docker exec $SCYLLA_CONTAINER cqlsh -e "DESCRIBE KEYSPACES;" 2>/dev/null | grep -c "cdr_keyspace" || true)
    
    if [ "$keyspace_exists" -gt 0 ]; then
        echo -e "${GREEN}✓ Keyspace 'cdr_keyspace' already exists${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠ Keyspace 'cdr_keyspace' does not exist${NC}"
        return 1
    fi
}

# Function to create schema
create_schema() {
    echo -e "${YELLOW}[3/4] Creating ScyllaDB schema...${NC}"
    
    # Check if schema file exists
    if [ ! -f "$SCHEMA_FILE" ]; then
        echo -e "${RED}✗ Schema file '$SCHEMA_FILE' not found!${NC}"
        echo -e "${YELLOW}Creating default schema...${NC}"
        
        # Create schema inline if file doesn't exist
        docker exec -i $SCYLLA_CONTAINER cqlsh <<EOF
CREATE KEYSPACE IF NOT EXISTS cdr_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cdr_keyspace;

CREATE TABLE IF NOT EXISTS daily_data_summary (
    msisdn TEXT,
    event_date DATE,
    data_type TEXT,
    total_up_bytes COUNTER,
    total_down_bytes COUNTER,
    total_cost_wak COUNTER,
    PRIMARY KEY ((msisdn), event_date, data_type)
);

CREATE TABLE IF NOT EXISTS daily_voice_summary (
    msisdn TEXT,
    event_date DATE,
    call_type TEXT,
    total_duration_sec COUNTER,
    total_cost_wak COUNTER,
    PRIMARY KEY ((msisdn), event_date, call_type)
);
EOF
    else
        # Copy and execute schema file
        docker cp "$SCHEMA_FILE" $SCYLLA_CONTAINER:/tmp/schema.cql
        docker exec $SCYLLA_CONTAINER cqlsh -f /tmp/schema.cql
    fi
    
    echo -e "${GREEN}✓ Schema created successfully${NC}"
}

# Function to verify schema
verify_schema() {
    echo -e "${YELLOW}Verifying schema...${NC}"
    
    docker exec $SCYLLA_CONTAINER cqlsh -e "DESCRIBE cdr_keyspace;" 2>/dev/null
    
    # Check if both tables exist
    local data_table=$(docker exec $SCYLLA_CONTAINER cqlsh -e "DESCRIBE cdr_keyspace.daily_data_summary;" 2>/dev/null | grep -c "CREATE TABLE" || true)
    local voice_table=$(docker exec $SCYLLA_CONTAINER cqlsh -e "DESCRIBE cdr_keyspace.daily_voice_summary;" 2>/dev/null | grep -c "CREATE TABLE" || true)
    
    if [ "$data_table" -gt 0 ] && [ "$voice_table" -gt 0 ]; then
        echo -e "${GREEN}✓ Both tables verified: daily_data_summary, daily_voice_summary${NC}"
    else
        echo -e "${RED}✗ Schema verification failed${NC}"
        exit 1
    fi
}

# Function to start stream processor
start_stream_processor() {
    echo -e "${YELLOW}[4/4] Starting stream processor...${NC}"
    
    docker-compose up -d stream-processor
    
    # Wait a moment for container to start
    sleep 3
    
    # Check if container is running
    if docker ps | grep -q "stream-processor"; then
        echo -e "${GREEN}✓ Stream processor started successfully${NC}"
        echo -e "${BLUE}View logs: docker-compose logs -f stream-processor${NC}"
    else
        echo -e "${RED}✗ Failed to start stream processor${NC}"
        docker-compose logs stream-processor
        exit 1
    fi
}

# Function to show status
show_status() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${GREEN}Setup Complete! Status:${NC}"
    echo -e "${BLUE}========================================${NC}"
    
    echo -e "\n${YELLOW}ScyllaDB Status:${NC}"
    docker exec $SCYLLA_CONTAINER nodetool status 2>/dev/null || echo "Unable to get status"
    
    echo -e "\n${YELLOW}Stream Processor Status:${NC}"
    docker ps --filter "name=stream-processor" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    
    echo -e "\n${YELLOW}Prometheus Metrics:${NC}"
    echo "http://localhost:8000/metrics"
    
    echo -e "\n${YELLOW}Useful Commands:${NC}"
    echo "  View logs:           docker-compose logs -f stream-processor"
    echo "  Query ScyllaDB:      docker exec -it scylla cqlsh"
    echo "  Check data:          docker exec -it scylla cqlsh -e 'SELECT * FROM cdr_keyspace.daily_data_summary LIMIT 5;'"
    echo "  Stop processor:      docker-compose stop stream-processor"
    echo "  Restart processor:   docker-compose restart stream-processor"
    
    echo -e "\n${BLUE}========================================${NC}"
}

# Main execution
main() {
    # Step 1: Wait for ScyllaDB
    wait_for_scylla
    
    # Step 2: Check if keyspace exists
    if ! check_keyspace; then
        # Step 3: Create schema if it doesn't exist
        create_schema
        verify_schema
    fi
    
    # Step 4: Start stream processor
    start_stream_processor
    
    # Show final status
    show_status
}

# Run the script
main "$@"
