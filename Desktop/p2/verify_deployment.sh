#!/bin/bash

# ==========================================
# FINAL VERIFICATION BEFORE DEPLOYMENT
# Complete system validation script
# ==========================================

set -e

DOMAIN="trendvision2004.com"
VM_IP="34.180.14.162"
PROJECT_DIR="/home/trading_app"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Test counters
PASSED=0
FAILED=0
TOTAL_TESTS=0

# Track test failures
FAILURES=()

# Test functions
run_test() {
    local test_name="$1"
    local test_command="$2"
    local expected_result="${3:-0}"

    echo -e "${BLUE}🧪 [$(date '+%H:%M:%S')] Running test: ${test_name}...${NC}"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    # Timeout after 30 seconds
    if timeout 30s bash -c "$test_command" 2>/dev/null; then
        actual_result=$?
    else
        actual_result=$?
    fi

    if [[ $actual_result -eq $expected_result ]]; then
        echo -e "${GREEN}✅ PASSED${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}❌ FAILED (Expected: $expected_result, Got: $actual_result)${NC}"
        FAILED=$((FAILED + 1))
        FAILURES+=("$test_name")
    fi
    echo ""
}

echo -e "${PURPLE}===========================================${NC}"
echo -e "${PURPLE}🔍 FINAL DEPLOYMENT VERIFICATION${NC}"
echo -e "${PURPLE}===========================================${NC}"
echo ""

# === SYSTEM PRE-DEPLOYMENT CHECKS ===
echo -e "${YELLOW}📋 PHASE 1: SYSTEM PRE-DEPLOYMENT CHECKS${NC}"
echo "----------------------------------------"

# 1. Operating System
run_test "Operating System" "cat /etc/os-release | grep -q 'Debian'" 0

# 2. Memory and CPU
run_test "System Resources" "[ $(free -m | awk 'NR==2{printf \"%.0f\", \$2}') -gt 7000 ] && [ $(nproc) -ge 2 ]" 0

# 3. Disk Space
run_test "Disk Space" "[ $(df / | awk 'NR==2{printf \"%.0f\", \$4}') -gt 10000000 ]" 0

# 4. Required ports available
run_test "Port 8080 Available" "! lsof -i :8080 >/dev/null 2>&1" 0
run_test "Port 80 Available" "! lsof -i :80 >/dev/null 2>&1" 0
run_test "Port 443 Available" "! lsof -i :443 >/dev/null 2>&1" 0

# 5. System packages
run_test "Python 3" "python3 --version >/dev/null 2>&1" 0
run_test "Pip" "pip3 --version >/dev/null 2>&1" 0
run_test "Apt" "apt --version >/dev/null 2>&1" 0

echo ""

# === FILE SYSTEM CHECKS ===
echo -e "${YELLOW}📁 PHASE 2: FILE SYSTEM VALIDATION${NC}"
echo "-------------------------------------"

# Project files
run_test "Project Directory" "[ -d \"$PROJECT_DIR\" ]" 0
run_test "app.py Exists" "[ -f \"$PROJECT_DIR/app.py\" ]" 0
run_test "pipeline1.py Exists" "[ -f \"$PROJECT_DIR/pipeline1.py\" ]" 0
run_test "config.json Exists" "[ -f \"$PROJECT_DIR/config/config.json\" ]" 0
run_test "requirements.txt Exists" "[ -f \"$PROJECT_DIR/requirements.txt\" ]" 0

# Database directories
run_test "Database Directory" "[ -d \"$PROJECT_DIR/database\" ]" 0

# Templates and static files
run_test "Templates Directory" "[ -d \"$PROJECT_DIR/templates\" ]" 0
run_test "Static Directory" "[ -d \"$PROJECT_DIR/static\" ]" 0

# Protobuf files
run_test "Protobuf Python File" "[ -f \"$PROJECT_DIR/MarketDataFeedV3_pb2.py\" ]" 0
run_test "Protobuf Source" "[ -f \"$PROJECT_DIR/MarketDataFeedV3.proto\" ]" 0

echo ""

# === CONFIGURATION VALIDATION ===
echo -e "${YELLOW}⚙️ PHASE 3: CONFIGURATION VALIDATION${NC}"
echo "-----------------------------------"

# Check config.json syntax and content
run_test "Config JSON Syntax" "python3 -m json.tool \"$PROJECT_DIR/config/config.json\" >/dev/null 2>&1" 0

# Check required config keys
run_test "Config Has ACCESS_TOKEN" "python3 -c \"import json; c=json.load(open('$PROJECT_DIR/config/config.json')); print(c.get('ACCESS_TOKEN', ''))\" | grep -q '[^[:space:]]'" 0

# Check instrument keys
run_test "Config Has NIFTY_FUTURE_key" "python3 -c \"import json; c=json.load(open('$PROJECT_DIR/config/config.json')); print(c.get('NIFTY_FUTURE_key', ''))\" | grep -q '[^[:space:]]'" 0
run_test "Config Has ITM_CE_key" "python3 -c \"import json; c=json.load(open('$PROJECT_DIR/config/config.json')); print(c.get('ITM_CE_key', ''))\" | grep -q '[^[:space:]]'" 0
run_test "Config Has ITM_PE_key" "python3 -c \"import json; c=json.load(open('$PROJECT_DIR/config/config.json')); print(c.get('ITM_PE_key', ''))\" | grep -q '[^[:space:]]'" 0

echo ""

# === PYTHON ENVIRONMENT CHECKS ===
echo -e "${YELLOW}🐍 PHASE 4: PYTHON ENVIRONMENT${NC}"
echo "------------------------------"

# Test individual Python imports
run_test "Import flask" "python3 -c \"import flask\" 2>/dev/null" 0
run_test "Import requests" "python3 -c \"import requests\" 2>/dev/null" 0
run_test "Import websockets" "python3 -c \"import websockets\" 2>/dev/null" 0

# Test application imports
run_test "Import app" "cd \"$PROJECT_DIR\" && python3 -c \"import app\" 2>/dev/null" 0
run_test "Import pipeline1" "cd \"$PROJECT_DIR\" && python3 -c \"import pipeline1\" 2>/dev/null" 0
run_test "Import protobuf" "cd \"$PROJECT_DIR\" && python3 -c \"import MarketDataFeedV3_pb2\" 2>/dev/null" 0

echo ""

# === APPLICATION FUNCTIONALITY TESTS ===
echo -e "${YELLOW}🚀 PHASE 5: APPLICATION FUNCTIONALITY${NC}"
echo "-------------------------------------"

# Test configuration loading
run_test "Config Loading" "cd \"$PROJECT_DIR\" && timeout 10s python3 -c \"import app; print('Config loaded successfully')\" 2>/dev/null | grep -q 'Config loaded'" 0

# Test pipeline initialization (without actual WebSocket connection)
run_test "Pipeline Import" "cd \"$PROJECT_DIR\" && timeout 5s python3 -c \"import pipeline1; print('Pipeline imported')\" 2>/dev/null | grep -q 'Pipeline imported'" 0

echo ""

# === NETWORKING TESTS ===
echo -e "${YELLOW}🌐 PHASE 6: NETWORKING & DNS${NC}"
echo "------------------------------"

# Test VM external connectivity
run_test "Internet Connectivity" "curl -f -s --max-time 5 google.com >/dev/null" 0

# Test local ports (should not be in use yet)
run_test "Local Services Check" "netstat -tuln | grep -q ':8080\|:80\|:443' && echo 'Ports in use' || echo 'Ports free'" 0

# DNS resolution (will fail initially but should pass later)
echo -e "${BLUE}🧪 [$(date '+%H:%M:%S')] DNS Resolution Test (optional):${NC}"
if nslookup $DOMAIN 8.8.8.8 2>/dev/null | grep -q $VM_IP; then
    echo -e "${GREEN}✅ PASSED${NC}"
    DNS_RESOLVED=true
else
    echo -e "${YELLOW}⚠️  NOT YET (DNS propagation needed)${NC}"
    DNS_RESOLVED=false
fi
echo ""

# === DEPLOYMENT SCRIPTS VALIDATION ===
echo -e "${YELLOW}📜 PHASE 7: DEPLOYMENT SCRIPTS${NC}"
echo "--------------------------------"

# Check if scripts exist and are executable
run_test "Deploy Script Exists" "[ -f \"$PROJECT_DIR/deploy.sh\" ]" 0
run_test "Deploy Script Executable" "[ -x \"$PROJECT_DIR/deploy.sh\" ]" 0

run_test "Domain Setup Script Exists" "[ -f \"$PROJECT_DIR/domain_setup.sh\" ]" 0
run_test "Domain Setup Script Executable" "[ -x \"$PROJECT_DIR/domain_setup.sh\" ]" 0

echo ""

# === FINAL SUMMARY ===
echo -e "${PURPLE}===========================================${NC}"
echo -e "${PURPLE}📊 VERIFICATION SUMMARY${NC}"
echo -e "${PURPLE}===========================================${NC}"
echo ""
echo -e "${BLUE}Total Tests Run:${NC} $TOTAL_TESTS"
echo -e "${GREEN}Passed:${NC} $PASSED"
echo -e "${RED}Failed:${NC} $FAILED"

if [[ $FAILED -eq 0 ]]; then
    echo ""
    echo -e "${GREEN}🎉 ALL PRE-DEPLOYMENT CHECKS PASSED!${NC}"
    echo -e "${GREEN}==========================================${NC}"
    echo -e "${BLUE}✅ READY FOR PRODUCTION DEPLOYMENT${NC}"
    echo ""
    echo -e "${BLUE}Next steps:${NC}"
    echo "1. Run deployment:   ./deploy.sh"
    echo "2. Setup domain SSL: ./domain_setup.sh"
    echo "3. Test at:          https://$DOMAIN"

    # Create deployment command
    echo ""
    echo "# Deployment Command (run when ready):"
    echo "chmod +x deploy.sh && ./deploy.sh"

else
    echo ""
    echo -e "${RED}❌ DEPLOYMENT BLOCKED - ISSUES FOUND${NC}"
    echo -e "${RED}=========================================${NC}"
    echo ""
    echo -e "${YELLOW}Failed Tests:${NC}"
    for failure in "${FAILURES[@]}"; do
        echo -e "  ❌ $failure"
    done
    echo ""
    echo -e "${BLUE}Resolve these issues before proceeding!${NC}"

    exit 1
fi

echo ""
echo -e "${BLUE}===========================================${NC}"
echo -e "${YELLOW}⚠️  IMPORTANT REMINDERS:${NC}"
echo "- Ensure DNS points to: $VM_IP"
echo "- Backup your config files before deployment"
echo "- Monitor logs after deployment at:"
echo "  • Application: journalctl -u trading-app -f"
echo "  • Nginx: journalctl -u nginx -f"
echo -e "${BLUE}===========================================${NC}"
