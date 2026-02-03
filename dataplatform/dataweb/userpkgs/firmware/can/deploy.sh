#!/bin/bash

# CAN Recompiler Library Deployment Script
# Builds the wheel and uploads to AWS S3 in both US and EU regions

set -e

S3_US_BUCKET="samsara-databricks-workspace"
S3_EU_BUCKET="samsara-eu-databricks-workspace"
S3_PATH="firmware/python_proto_latest/wheels/"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🏗️  Building CAN Recompiler Library${NC}"

# Clean and build
make clean
make build

# Check if wheel was created
WHEEL_FILE=$(find dist/ -name "*.whl" | head -1)
if [ -z "$WHEEL_FILE" ]; then
    echo -e "${RED}❌ Error: No wheel file found in dist/!${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Built wheel: $(basename "$WHEEL_FILE")${NC}"

# Function to upload to S3
upload_to_s3() {
    local bucket=$1
    local profile=$2
    local region_name=$3
    
    echo -e "${YELLOW}🚀 Uploading to $region_name region ($bucket)...${NC}"
    
    # Check if AWS profile exists
    if ! aws configure list-profiles | grep -q "^$profile$"; then
        echo -e "${RED}❌ Error: AWS profile '$profile' not found!${NC}"
        echo -e "${YELLOW}Available profiles:${NC}"
        aws configure list-profiles
        return 1
    fi
    
    # Upload wheel to S3
    if aws s3 cp "$WHEEL_FILE" "s3://$bucket/$S3_PATH" \
        --profile "$profile" \
        --no-progress; then
        echo -e "${GREEN}✅ Successfully uploaded to $region_name region${NC}"
        echo -e "${BLUE}   URL: s3://$bucket/$S3_PATH$(basename "$WHEEL_FILE")${NC}"
    else
        echo -e "${RED}❌ Failed to upload to $region_name region${NC}"
        return 1
    fi
}

# Upload to both regions
echo -e "${BLUE}📦 Deploying to AWS S3...${NC}"

# US Region
upload_to_s3 "$S3_US_BUCKET" "firmwareadmin" "US"

# EU Region  
upload_to_s3 "$S3_EU_BUCKET" "eu-firmwareadmin" "EU"

echo -e "${GREEN}🎉 Deployment complete!${NC}"
echo -e "${BLUE}Package can be installed from S3 using:${NC}"
echo -e "${YELLOW}  pip install s3://$S3_US_BUCKET/$S3_PATH$(basename "$WHEEL_FILE")${NC}"
echo -e "${YELLOW}  pip install s3://$S3_EU_BUCKET/$S3_PATH$(basename "$WHEEL_FILE")${NC}"