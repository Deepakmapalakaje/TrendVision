#!/bin/bash
# TrendVision Complete Deployment Script for Mumbai VM
# VM: instance-20250921-070759 (34.93.47.90)
# Domain: trendvision2004.com

echo "🚀 TrendVision Deployment Starting..."
echo "📍 Target: Mumbai VM (34.93.47.90)"
echo "🌐 Domain: trendvision2004.com"

# Update system
sudo apt update && sudo apt upgrade -y

# Install system packages
sudo apt install -y python3 python3-pip python3-venv nginx certbot python3-certbot-nginx git htop curl

# Create system user
sudo adduser --system --group --no-create-home trendvision || true

# Setup application directory
sudo mkdir -p /opt/trendvision
sudo chown $USER:$USER /opt/trendvision
cd /opt/trendvision

# Clone repository (if not already copied)
if [ ! -f "app.py" ]; then
    git clone https://github.com/Deepakmapalakaje/TrendVision.git .
fi

# Create virtual environment
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate

# Create directories
mkdir -p config database logs

# Create production config
cat > config/config.json << 'EOF'
{
  "ACCESS_TOKEN": "update-daily-in-admin-panel",
  "NIFTY_FUTURE_key": "NSE_FO|53001"
}
EOF

# Create environment file
cat > .env << 'EOF'
FLASK_SECRET_KEY=trendvision-super-secure-production-key-2024
TRADING_DB=database/upstox_v3_live_trading.db
USER_DB=database/users.db
PORT=8080
FLASK_ENV=production
EOF

# Create systemd service for web app
sudo tee /etc/systemd/system/trendvision-web.service << 'EOF'
[Unit]
Description=TrendVision Web Application
After=network.target

[Service]
Type=simple
User=trendvision
Group=trendvision
WorkingDirectory=/opt/trendvision
Environment=PATH=/opt/trendvision/venv/bin
ExecStart=/opt/trendvision/venv/bin/python app.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Create systemd service for pipeline
sudo tee /etc/systemd/system/trendvision-pipeline.service << 'EOF'
[Unit]
Description=TrendVision Trading Pipeline
After=network.target

[Service]
Type=simple
User=trendvision
Group=trendvision
WorkingDirectory=/opt/trendvision
Environment=PATH=/opt/trendvision/venv/bin
ExecStart=/opt/trendvision/venv/bin/python pipeline1.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Create nginx configuration
sudo tee /etc/nginx/sites-available/trendvision2004.com << 'EOF'
server {
    listen 80;
    server_name trendvision2004.com www.trendvision2004.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    location /static/ {
        alias /opt/trendvision/static/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
EOF

# Enable nginx site
sudo ln -s /etc/nginx/sites-available/trendvision2004.com /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl restart nginx

# Set permissions
sudo chown -R trendvision:trendvision /opt/trendvision

# Enable and start services
sudo systemctl daemon-reload
sudo systemctl enable trendvision-web
sudo systemctl enable trendvision-pipeline
sudo systemctl start trendvision-web
sudo systemctl start trendvision-pipeline

# Setup SSL certificate
sudo certbot --nginx -d trendvision2004.com -d www.trendvision2004.com --email admin@trendvision2004.com --agree-tos --non-interactive

# Setup log rotation
sudo tee /etc/logrotate.d/trendvision << 'EOF'
/opt/trendvision/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 trendvision trendvision
}
EOF

echo "✅ TrendVision Deployment Completed!"
echo "🌐 Access: https://trendvision2004.com"
echo "🔧 Admin: https://trendvision2004.com/admin/login (dsar/dsar)"
echo "📊 Status: sudo systemctl status trendvision-web trendvision-pipeline"
</content>
