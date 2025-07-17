#!/bin/bash

# CleanClinic Bronze to Silver Processing Trigger
# This script triggers the processing of Parquet files from bronze to silver
# Silver = UMLS Mapper (Delta table) + Geo Enrich + PII Scrub

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "scripts/process_bronze_to_silver.py" ]; then
    print_error "This script must be run from the CleanClinic project root directory"
    exit 1
fi

# Check if bronze directory exists and has Parquet files
BRONZE_DIR="workspace/bronze"
if [ ! -d "$BRONZE_DIR" ]; then
    print_error "Bronze directory not found: $BRONZE_DIR"
    print_status "Please run the ingest process first to populate the bronze directory"
    exit 1
fi

# Count Parquet files in bronze
PARQUET_COUNT=$(find "$BRONZE_DIR" -name "*.parquet" | wc -l | tr -d ' ')
if [ "$PARQUET_COUNT" -eq 0 ]; then
    print_warning "No Parquet files found in bronze directory"
    print_status "Please run the ingest process first to populate the bronze directory"
    exit 1
fi

print_status "Found $PARQUET_COUNT Parquet files in bronze directory"

# Check if Python script is executable
if [ ! -x "scripts/process_bronze_to_silver.py" ]; then
    print_status "Making script executable..."
    chmod +x scripts/process_bronze_to_silver.py
fi

# Check for required environment variables
print_status "Checking environment variables..."

if [ -z "$UMLS_API_KEY" ]; then
    print_warning "UMLS_API_KEY environment variable not set"
    print_status "UMLS mapping will be skipped"
else
    print_success "UMLS API key found"
fi

if [ -z "$GEO_API_KEY" ]; then
    print_warning "GEO_API_KEY environment variable not set"
    print_status "Geo enrichment will use free services only"
else
    print_success "Geo API key found"
fi

# Create silver directory if it doesn't exist
SILVER_DIR="workspace/silver"
mkdir -p "$SILVER_DIR"

print_status "Starting bronze to silver processing..."
print_status "Bronze directory: $BRONZE_DIR"
print_status "Silver directory: $SILVER_DIR"
print_status "Processing pipeline: PII Scrub → Geo Enrich → UMLS Map → Delta Format"

# Run the processing script
python scripts/process_bronze_to_silver.py \
    --bronze-dir "$BRONZE_DIR" \
    --silver-dir "$SILVER_DIR" \
    --pii-mode remove \
    --delta-format

# Check if processing was successful
if [ $? -eq 0 ]; then
    print_success "Bronze to silver processing completed successfully!"
    
    # Count processed files
    DELTA_COUNT=$(find "$SILVER_DIR" -name "silver_*" -type d | wc -l | tr -d ' ')
    PARQUET_COUNT=$(find "$SILVER_DIR" -name "processed_*.parquet" | wc -l | tr -d ' ')
    TOTAL_PROCESSED=$((DELTA_COUNT + PARQUET_COUNT))
    
    print_status "Processed files: $TOTAL_PROCESSED"
    
    # List processed files
    if [ "$DELTA_COUNT" -gt 0 ]; then
        print_status "Delta tables created:"
        find "$SILVER_DIR" -name "silver_*" -type d -exec basename {} \;
    fi
    
    if [ "$PARQUET_COUNT" -gt 0 ]; then
        print_status "Parquet files created:"
        find "$SILVER_DIR" -name "processed_*.parquet" -exec basename {} \;
    fi
    
    # Show summary files
    SUMMARY_COUNT=$(find "$SILVER_DIR" -name "*.summary.yaml" | wc -l | tr -d ' ')
    if [ "$SUMMARY_COUNT" -gt 0 ]; then
        print_status "Transformation summaries: $SUMMARY_COUNT"
    fi
    
    print_status "Next step: Run audit process on silver data"
    print_status "Command: ./scripts/audit.py --input-dir $SILVER_DIR"
    
else
    print_error "Bronze to silver processing failed"
    exit 1
fi 