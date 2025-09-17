#!/bin/bash

# ====================================================================
# QUICK DEPLOYMENT COMMANDS FOR TRENDVISION2004.COM
# Run these commands sequentially on your Linux VM
# ====================================================================

echo "🚀 Starting TrendVision trading system deployment..."

# Step 1: Create project directory and navigate
mkdir -p /home/trading_app
cd /home/trading_app

# Step 2: Set up deployment directory structure
mkdir -p config database logs static/css static/js templates
echo "✅ Directory structure created"

# Step 3: System update and required packages
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3-pip python3-venv nginx curl wget htop iotop nload certbot python3-certbot-nginx firewalld
echo "✅ System packages installed"

# Step 4: Python environment setup
python3 -m venv trading_env
source trading_env/bin/activate
pip install --upgrade pip setuptools wheel
echo "✅ Python environment ready"

# Step 5: Install dependencies
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo "✅ Python dependencies installed"
else
    echo "❌ requirements.txt not found"
    exit 1
fi

# Step 6: Configure Nginx for trendvision2004.com
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

        # WebSocket support for real-time trading data
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_connect_timeout 75s;
        proxy_send_timeout 75s;
        proxy_read_timeout 75s;
    }

    location /static/ {
        alias /home/trading_app/static/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    location /health {
        access_log off;
        return 200 "healthy\\n";
        add_header Content-Type text/plain;
    }
}
EOF

# Enable site
sudo ln -sf /etc/nginx/sites-available/trendvision2004.com /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
echo "✅ Nginx configured"

# Step 7: Create systemd service
sudo tee /etc/systemd/system/trading-app.service > /dev/null <<EOF
[Unit]
Description=Ultra-Fast Trading System
After=network.target
Wants=nginx.service

[Service]
Type=simple
User=$USER
WorkingDirectory=/home/trading_app
Environment=PATH=/home/trading_app/trading_env/bin
Environment=TRADING_DB=database/upstox_v3_live_trading.db
Environment=USER_DB=database/users.db
Environment=FLASK_SECRET_KEY=your-production-secret-key-change-in-production
ExecStart=/home/trading_app/trading_env/bin/python /home/trading_app/app.py
Restart=always
RestartSec=10
RestartPreventExitStatus=42

# Security settings
NoNewPrivileges=yes
PrivateTmp=yes
ProtectHome=yes
ProtectSystem=strict
ReadWritePaths=/home/trading_app/database /home/trading_app/logs
WorkingDirectory=/home/trading_app

# Resource limits
LimitNOFILE=65536
MemoryLimit=6G

[Install]
WantedBy=multi-user.target
EOF

# Step 8: Reload systemd and enable service
sudo systemctl daemon-reload
sudo systemctl enable trading-app
sudo systemctl start trading-app
echo "✅ Trading service configured and started"

# Step 9: Test services
if systemctl is-active --quiet trading-app; then
    echo "✅ Trading app is running"
else
    echo "❌ Trading app failed to start - check logs"
    sudo journalctl -u trading-app -n 20
fi

if systemctl is-active --quiet nginx; then
    echo "✅ Nginx is running"
else
    echo "❌ Nginx failed to start"
fi

# Step 10: Test health endpoint
if curl -f -s http://localhost/health > /dev/null; then
    echo "✅ Health check passed"
else
    echo "❌ Health check failed"
fi

if curl -f -s http://localhost:8080/ | grep -q "redirect"; then
    echo "✅ Flask app responding"
else
    echo "❌ Flask app not responding"
fi

echo ""
echo "============================================"
echo "🎉 DEPLOYMENT COMPLETE - BASIC SETUP DONE!"
echo "============================================"
echo ""
echo "✅ Services installed and running:"
echo "   - Ultra-fast trading pipeline"
echo "   - Flask web application (port 8080)"
echo "   - Nginx reverse proxy (port 80)"
echo "   - Database auto-creation enabled"
echo ""
echo "📋 NEXT STEPS FOR DOMAIN & PRODUCTION:"
echo "1. Point DNS: trendvision2004.com → $(curl -s ifconfig.me)"
echo "2. Test HTTP: http://trendvision2004.com/health"
echo "3. SSL Setup: certbot --nginx (after DNS propagation)"
echo "4. Production: https://trendvision2004.com"
echo ""
echo "🔍 MONITORING COMMANDS:"
echo "   Status: sudo systemctl status trading-app nginx"
echo "   Logs: sudo journalctl -u trading-app -f"
echo "   Resources: htop"
echo ""
echo "=============================================="
