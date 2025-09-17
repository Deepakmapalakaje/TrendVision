#!/bin/bash

# ==========================================
# ULTRA-FAST TRADING SYSTEM DEPLOYMENT
# for trendvision2004.com
# ==========================================

set -e

echo "🚀 Starting deployment for trendvision2004.com..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DOMAIN="trendvision2004.com"
PROJECT_DIR="/home/trading_app"
VM_IP="34.180.14.162"

# Functions
check_system() {
    echo -e "${BLUE}🔍 Checking system requirements...${NC}"
    if [[ "$EUID" -eq 0 ]]; then
        echo -e "${RED}❌ Do not run as root${NC}"
        exit 1
    fi
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}❌ Python3 not found${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ System check passed${NC}"
}

update_system() {
    echo -e "${BLUE}📦 Updating system packages...${NC}"
    sudo apt update && sudo apt upgrade -y
    sudo apt install -y curl wget git htop monitor
    echo -e "${GREEN}✅ System updated${NC}"
}

setup_python_env() {
    echo -e "${BLUE}🐍 Setting up Python environment...${NC}"

    # Install Python pip if not present
    if ! command -v pip3 &> /dev/null; then
        sudo apt install -y python3-pip
    fi

    # Create virtual environment
    mkdir -p "$PROJECT_DIR"
    cd "$PROJECT_DIR"
    python3 -m venv trading_env
    source trading_env/bin/activate

    # Upgrade pip and install requirements
    pip install --upgrade pip setuptools wheel
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
    else
        echo -e "${RED}❌ requirements.txt not found${NC}"
        exit 1
    fi

    echo -e "${GREEN}✅ Python environment ready${NC}"
}

setup_directories() {
    echo -e "${BLUE}📁 Setting up directories...${NC}"

    cd "$PROJECT_DIR"
    mkdir -p config database logs static/css static/js templates

    # Set proper permissions
    mkdir -p ~/.ssh
    chmod 700 ~/.ssh

    echo -e "${GREEN}✅ Directories created${NC}"
}

download_project_files() {
    echo -e "${BLUE}📥 Downloading project files...${NC}"

    # This assumes files are transferred via scp/rsync
    # In a real scenario, you'd download from git or transfer files

    echo -e "${GREEN}✅ Project files ready${NC}"
}

configure_nginx() {
    echo -e "${BLUE}🌐 Configuring Nginx for trendvision2004.com...${NC}"

    # Install Nginx
    sudo apt install -y nginx

    # Create Nginx configuration
    sudo tee /etc/nginx/sites-available/trendvision2004.com > /dev/null <<EOF
server {
    listen 80;
    server_name trendvision2004.com www.trendvision2004.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";

        # Timeout settings for long-running connections
        proxy_connect_timeout 75s;
        proxy_send_timeout 75s;
        proxy_read_timeout 75s;
    }

    # Static files
    location /static/ {
        alias $PROJECT_DIR/static/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    # Health check
    location /health {
        access_log off;
        return 200 "healthy\n";
        add_header Content-Type text/plain;
    }
}
EOF

    # Enable site and remove default
    sudo ln -sf /etc/nginx/sites-available/trendvision2004.com /etc/nginx/sites-enabled/
    sudo rm -f /etc/nginx/sites-enabled/default

    # Test configuration
    sudo nginx -t

    echo -e "${GREEN}✅ Nginx configured${NC}"
}

configure_ssl() {
    echo -e "${BLUE}🔒 Setting up SSL certificates...${NC}"

    # Install certbot
    sudo apt install -y certbot python3-certbot-nginx

    # Note: SSL will be configured after domain points to server
    echo -e "${YELLOW}⚠️  SSL will be configured manually after DNS propagation${NC}"

    echo -e "${GREEN}✅ SSL setup prepared${NC}"
}

create_service() {
    echo -e "${BLUE}⚙️ Creating systemd service...${NC}"

    # Create service file
    sudo tee /etc/systemd/system/trading-app.service > /dev/null <<EOF
[Unit]
Description=Ultra-Fast Trading System
After=network.target
Wants=nginx.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$PROJECT_DIR
Environment=PATH=$PROJECT_DIR/trading_env/bin
Environment=TRADING_DB=database/upstox_v3_live_trading.db
Environment=USER_DB=database/users.db
Environment=FLASK_SECRET_KEY=your-super-secret-key-change-this
ExecStart=$PROJECT_DIR/trading_env/bin/python $PROJECT_DIR/app.py
Restart=always
RestartSec=10
RestartPreventExitStatus=42

# Security settings
NoNewPrivileges=yes
PrivateTmp=yes
ProtectHome=yes
ProtectSystem=strict
ReadWritePaths=$PROJECT_DIR/database $PROJECT_DIR/logs
WorkingDirectory=$PROJECT_DIR

# Resource limits
LimitNOFILE=65536
MemoryLimit=6G

[Install]
WantedBy=multi-user.target
EOF

    echo -e "${GREEN}✅ Service created${NC}"
}

setup_monitoring() {
    echo -e "${BLUE}📊 Setting up monitoring...${NC}"

    # Install monitoring tools
    sudo apt install -y htop iotop nload

    # Create log rotation
    sudo tee /etc/logrotate.d/trading-app > /dev/null <<EOF
$PROJECT_DIR/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 0644 $USER $USER
    postrotate
        systemctl reload trading-app.service || true
    endscript
}
EOF

    echo -e "${GREEN}✅ Monitoring configured${NC}"
}

setup_firewall() {
    echo -e "${BLUE}🔥 Configuring firewall...${NC}"

    # Enable firewalld
    sudo apt install -y firewalld
    sudo systemctl enable firewalld
    sudo systemctl start firewalld

    # Configure rules
    sudo firewall-cmd --permanent --add-service=http
    sudo firewall-cmd --permanent --add-service=https
    sudo firewall-cmd --permanent --add-port=8080/tcp
    sudo firewall-cmd --reload

    echo -e "${GREEN}✅ Firewall configured${NC}"
}

pre_deployment_checks() {
    echo -e "${BLUE}🧪 Running pre-deployment checks...${NC}"

    cd "$PROJECT_DIR"

    # Check Python environment
    source trading_env/bin/activate
    if ! python -c "import flask, websockets, schedule"; then
        echo -e "${RED}❌ Python dependencies not installed correctly${NC}"
        exit 1
    fi

    # Check configuration files
    if [[ ! -f "config/config.json" ]]; then
        echo -e "${RED}❌ config.json not found${NC}"
        exit 1
    fi

    # Create database directory if not exists
    mkdir -p database

    echo -e "${GREEN}✅ Pre-deployment checks passed${NC}"
}

test_app() {
    echo -e "${BLUE}🧪 Testing application...${NC}"

    cd "$PROJECT_DIR"
    source trading_env/bin/activate

    # Test configuration loading
    if ! python -c "
import sys
sys.path.append('.')
try:
    with open('config/config.json', 'r') as f:
        import json
        config = json.load(f)
    print('✅ Config loaded successfully')
    if 'ACCESS_TOKEN' in config and config['ACCESS_TOKEN']:
        print('✅ Access token found')
    else:
        print('❌ Access token missing')
        sys.exit(1)
except Exception as e:
    print(f'❌ Config error: {e}')
    sys.exit(1)
    "; then
        echo -e "${RED}❌ Application test failed${NC}"
        exit 1
    fi

    echo -e "${GREEN}✅ Application tests passed${NC}"
}

start_services() {
    echo -e "${BLUE}🚀 Starting services...${NC}"

    # Reload systemd and start services
    sudo systemctl daemon-reload
    sudo systemctl enable trading-app
    sudo systemctl start trading-app
    sudo systemctl enable nginx
    sudo systemctl start nginx

    # Wait for services to start
    sleep 5

    # Check service status
    if systemctl is-active --quiet trading-app; then
        echo -e "${GREEN}✅ Trading app started successfully${NC}"
    else
        echo -e "${RED}❌ Trading app failed to start${NC}"
        exit 1
    fi

    if systemctl is-active --quiet nginx; then
        echo -e "${GREEN}✅ Nginx started successfully${NC}"
    else
        echo -e "${RED}❌ Nginx failed to start${NC}"
        exit 1
    fi
}

verify_deployment() {
    echo -e "${BLUE}🔍 Verifying deployment...${NC}"

    # Test health endpoint
    if curl -f -s http://localhost/health > /dev/null; then
        echo -e "${GREEN}✅ Health check passed${NC}"
    else
        echo -e "${RED}❌ Health check failed${NC}"
        exit 1
    fi

    # Test app accessibility
    if curl -f -s http://localhost:8080/ | grep -q "redirect"; then
        echo -e "${GREEN}✅ Flask app responding${NC}"
    else
        echo -e "${RED}❌ Flask app not responding${NC}"
        exit 1
    fi

    echo -e "${GREEN}✅ Deployment verification successful${NC}"
}

main() {
    echo -e "${BLUE}===========================================${NC}"
    echo -e "${BLUE}🚀 TRENDVISION2004.COM DEPLOYMENT STARTED${NC}"
    echo -e "${BLUE}===========================================${NC}"
    echo ""

    check_system
    update_system
    setup_python_env
    setup_directories
    configure_nginx
    configure_ssl
    create_service
    setup_monitoring
    setup_firewall
    pre_deployment_checks
    test_app
    start_services
    verify_deployment

    echo ""
    echo -e "${GREEN}===========================================${NC}"
    echo -e "${GREEN}🎉 DEPLOYMENT COMPLETED SUCCESSFULLY!${NC}"
    echo -e "${GREEN}===========================================${NC}"
    echo ""
    echo -e "${BLUE}📋 Next Steps:${NC}"
    echo -e "${BLUE}1. Point domain trendvision2004.com to: $VM_IP${NC}"
    echo -e "${BLUE}2. Wait for DNS propagation (~30 mins)${NC}"
    echo -e "${BLUE}3. Run SSL setup: sudo certbot --nginx${NC}"
    echo -e "${BLUE}4. Test: https://trendvision2004.com${NC}"
    echo ""
    echo -e "${BLUE}🎯 Service Status:${NC}"
    sudo systemctl status trading-app --no-pager
    sudo systemctl status nginx --no-pager
}

# Run main function
if [[ "$1" == "--test" ]]; then
    echo "TEST MODE - Running without system changes"
    check_system
    test_app
    echo "✅ Test completed successfully"
elif [[ "$1" == "--rollback" ]]; then
    echo "ROLLBACK MODE - Stopping and removing services"
    sudo systemctl stop trading-app nginx
    sudo systemctl disable trading-app nginx
    sudo rm -f /etc/nginx/sites-enabled/trendvision2004.com
    sudo rm -f /etc/systemd/system/trading-app.service
    sudo systemctl daemon-reload
    sudo systemctl restart nginx
    echo "✅ Rollback completed"
else
    main
fi
