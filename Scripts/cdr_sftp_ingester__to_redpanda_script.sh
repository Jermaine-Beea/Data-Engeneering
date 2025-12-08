#!/bin/bash

# SFTP to Redpanda Ingester Script
# Downloads CDR files from SFTP and streams them to Redpanda

set -e  # Exit on error

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}CDR SFTP to Redpanda Ingester${NC}"
echo -e "${BLUE}========================================${NC}"

# Configuration
CONTAINER_NAME="cdr-ingester"
SERVICE_NAME="cdr-ingester"

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}[1/4] Checking prerequisites...${NC}"
    
    # Check if SFTP is running
    if ! docker ps | grep -q "sftp"; then
        echo -e "${RED}✗ SFTP server is not running${NC}"
        echo -e "${YELLOW}Starting SFTP server...${NC}"
        docker-compose up -d sftp
        sleep 5
    else
        echo -e "${GREEN}✓ SFTP server is running${NC}"
    fi
    
    # Check if Redpanda is healthy
    if ! docker exec redpanda-0 rpk cluster health 2>/dev/null | grep -q "Healthy.*true"; then
        echo -e "${RED}✗ Redpanda cluster is not healthy${NC}"
        echo -e "${YELLOW}Please ensure Redpanda is running: docker-compose up -d redpanda-0 redpanda-1 redpanda-2${NC}"
        exit 1
    else
        echo -e "${GREEN}✓ Redpanda cluster is healthy${NC}"
    fi
    
    # Check if there are files on SFTP
    local file_count=$(docker exec sftp ls /home/cdr_data/ 2>/dev/null | grep -c "cdr_.*\.csv" || echo "0")
    if [ "$file_count" -eq 0 ]; then
        echo -e "${YELLOW}⚠ No CDR files found on SFTP server${NC}"
        echo -e "${YELLOW}Tip: Run the CDR generator first to create files${NC}"
    else
        echo -e "${GREEN}✓ Found $file_count CDR files on SFTP server${NC}"
    fi
}

# Function to create Redpanda topics if they don't exist
create_topics() {
    echo -e "${YELLOW}[2/4] Creating Redpanda topics...${NC}"
    
    # Create cdr-data topic
    if ! docker exec redpanda-0 rpk topic list 2>/dev/null | grep -q "cdr-data"; then
        docker exec redpanda-0 rpk topic create cdr-data --partitions 3 --replicas 3
        echo -e "${GREEN}✓ Created topic: cdr-data${NC}"
    else
        echo -e "${GREEN}✓ Topic already exists: cdr-data${NC}"
    fi
    
    # Create cdr-voice topic
    if ! docker exec redpanda-0 rpk topic list 2>/dev/null | grep -q "cdr-voice"; then
        docker exec redpanda-0 rpk topic create cdr-voice --partitions 3 --replicas 3
        echo -e "${GREEN}✓ Created topic: cdr-voice${NC}"
    else
        echo -e "${GREEN}✓ Topic already exists: cdr-voice${NC}"
    fi
}

# Function to start the ingester
start_ingester() {
    echo -e "${YELLOW}[3/4] Starting CDR ingester...${NC}"
    
    # Stop existing container if running
    if docker ps -a | grep -q "$CONTAINER_NAME"; then
        echo -e "${YELLOW}Stopping existing ingester...${NC}"
        docker stop "$CONTAINER_NAME" 2>/dev/null || true
        docker rm "$CONTAINER_NAME" 2>/dev/null || true
    fi
    
    # Start the ingester service
    docker-compose up -d "$SERVICE_NAME"
    
    # Wait for container to start
    sleep 3
    
    # Check if container is running
    if docker ps | grep -q "$CONTAINER_NAME"; then
        echo -e "${GREEN}✓ CDR ingester started successfully${NC}"
    else
        echo -e "${RED}✗ Failed to start CDR ingester${NC}"
        docker-compose logs "$SERVICE_NAME"
        exit 1
    fi
}

# Function to monitor ingestion
monitor_ingestion() {
    echo -e "${YELLOW}[4/4] Monitoring ingestion...${NC}"
    echo -e "${BLUE}========================================${NC}"
    
    # Follow logs for 10 seconds
    timeout 10s docker-compose logs -f "$SERVICE_NAME" 2>/dev/null || true
    
    echo -e "${BLUE}========================================${NC}"
    echo -e "${GREEN}Ingestion is running!${NC}"
    echo ""
}

# Function to show status and useful commands
show_status() {
    echo -e "${YELLOW}Status:${NC}"
    docker ps --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    
    echo -e "\n${YELLOW}Redpanda Topics:${NC}"
    docker exec redpanda-0 rpk topic list 2>/dev/null | grep "cdr-" || echo "No CDR topics found"
    
    echo -e "\n${YELLOW}Records in Topics:${NC}"
    local data_count=$(docker exec redpanda-0 rpk topic describe cdr-data 2>/dev/null | grep "High watermark" | awk '{sum+=$3} END {print sum}' || echo "0")
    local voice_count=$(docker exec redpanda-0 rpk topic describe cdr-voice 2>/dev/null | grep "High watermark" | awk '{sum+=$3} END {print sum}' || echo "0")
    echo "  cdr-data:  $data_count records"
    echo "  cdr-voice: $voice_count records"
    
    echo -e "\n${YELLOW}Useful Commands:${NC}"
    echo "  View live logs:        docker-compose logs -f $SERVICE_NAME"
    echo "  Check processed files: docker exec $CONTAINER_NAME cat /app/processed_files.txt"
    echo "  Restart ingester:      docker-compose restart $SERVICE_NAME"
    echo "  Stop ingester:         docker-compose stop $SERVICE_NAME"
    echo "  View topic messages:   docker exec redpanda-0 rpk topic consume cdr-data --num 10"
    
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${GREEN}CDR Ingestion Pipeline Running!${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Main execution
main() {
    check_prerequisites
    create_topics
    start_ingester
    monitor_ingestion
    show_status
}

# Run the script
main "$@"
