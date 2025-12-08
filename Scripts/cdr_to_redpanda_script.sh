#!/bin/bash

# CDR SFTP to Redpanda Pipeline Script
# Executes the CDR data generator that downloads from SFTP and streams to Redpanda

set -e  # Exit on error

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}CDR SFTP to Redpanda Pipeline${NC}"
echo -e "${BLUE}========================================${NC}"

# Run the CDR container
echo -e "${GREEN}Starting CDR pipeline...${NC}"
docker-compose up cdr

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Pipeline execution complete!${NC}"
echo -e "${BLUE}========================================${NC}"
