#!/bin/bash

set -euo pipefail

VM_NAME="instance-20250921-125329"
ZONE="asia-south1-b"
DOMAIN="trendvision2004.com"
ALT_DOMAIN="www.trendvision2004.com"
VM_IP="34.93.113.19"
REPO_URL="https://github.com/Deepakmapalakaje/TrendVision.git"

log_step() { printf '\n==> %s\n' "$1"; }
log_info() { printf '    %s\n' "$1"; }
log_warn() { printf 'WARN: %s\n' "$1"; }
log_success() { printf 'SUCCESS: %s\n' "$1"; }

require_binary() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

run_remote() {
    local remote_cmd="$1"
    gcloud compute ssh "$VM_NAME" --zone="$ZONE" --command "$remote_cmd"
}

require_binary gcloud
require_binary nslookup
require_binary curl

log_step "Pre-check: DNS propagation for $DOMAIN"
if nslookup "$DOMAIN" 8.8.8.8 | grep -q "$VM_IP"; then
    log_success "DNS already resolves to $VM_IP"
else
    log_warn "DNS does not yet resolve to $VM_IP. Update DNS records and wait for propagation before rerunning."
fi

log_step "Step 1: Update package index and upgrade base system"
run_remote "sudo apt update && sudo apt upgrade -y"
log_success "System packages updated"

log_step "Step 2: Install required system packages"
run_remote "sudo apt install -y python3 python3-pip python3-venv nginx certbot python3-certbot-nginx git htop curl unzip build-essential"
log_success "System packages installed"

log_step "Step 3: Create dedicated system user"
run_remote "sudo adduser --system --group --no-create-home trendvision"
log_success "System user created"

log_step "Step 4: Prepare application directory"
run_remote "sudo mkdir -p /opt/trendvision && sudo chown \$USER:\$USER /opt/trendvision"
log_success "Application directory prepared"

log_step "Step 5: Clone TrendVision repository"
run_remote "cd /opt/trendvision && git clone $REPO_URL ."
log_success "Repository cloned"

log_step "Step 6: Create virtual environment and install dependencies"
run_remote "cd /opt/trendvision && python3 -m venv venv && source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt && deactivate"
log_success "Virtual environment ready"

log_step "Step 7: Create application support directories"
run_remote "cd /opt/trendvision && mkdir -p config database logs"
log_success "Support directories created"

log_step "Step 8: Create production configuration file"
run_remote "cd /opt/trendvision && cat > config/config.json << 'EOF'
{
  \"ACCESS_TOKEN\": \"update-daily-in-admin-panel\",
  \"NIFTY_FUTURE_key\": \"NSE_FO|53001\"
}
EOF"
log_success "config/config.json created"

log_step "Step 9: Create environment file"
run_remote "cd /opt/trendvision && cat > .env << 'EOF'
FLASK_SECRET_KEY=trendvision-enhanced-production-key-2024
TRADING_DB=database/upstox_v3_live_trading.db
USER_DB=database/users.db
PORT=8080
FLASK_ENV=production
RUN_PIPELINE=1
SENDER_EMAIL=dscatreeing@gmail.com
SENDER_PASSWORD=lszl urfy lhlm vshz
EOF"
log_success ".env file created"

log_step "Step 10: Create systemd service for the web application"
run_remote "sudo tee /etc/systemd/system/trendvision-web.service << 'EOF'
[Unit]
Description=TrendVision Web Application - Enhanced Version
After=network.target

[Service]
Type=simple
User=trendvision
Group=trendvision
WorkingDirectory=/opt/trendvision
Environment=PATH=/opt/trendvision/venv/bin
Environment=PYTHONPATH=/opt/trendvision
ExecStart=/opt/trendvision/venv/bin/python app.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
KillMode=mixed
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF"
log_success "trendvision-web.service created"

log_step "Step 11: Create systemd service for the trading pipeline"
run_remote "sudo tee /etc/systemd/system/trendvision-pipeline.service << 'EOF'
[Unit]
Description=TrendVision Trading Pipeline - Enhanced Version
After=network.target

[Service]
Type=simple
User=trendvision
Group=trendvision
WorkingDirectory=/opt/trendvision
Environment=PATH=/opt/trendvision/venv/bin
Environment=PYTHONPATH=/opt/trendvision
ExecStart=/opt/trendvision/venv/bin/python pipeline1.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
KillMode=mixed
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF"
log_success "trendvision-pipeline.service created"

log_step "Step 12: Configure nginx"
run_remote "sudo tee /etc/nginx/sites-available/trendvision2004.com << 'EOF'
server {
    listen 80;
    server_name $DOMAIN $ALT_DOMAIN;

    add_header X-Frame-Options \"SAMEORIGIN\" always;
    add_header X-XSS-Protection \"1; mode=block\" always;
    add_header X-Content-Type-Options \"nosniff\" always;
    add_header Referrer-Policy \"no-referrer-when-downgrade\" always;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header X-Forwarded-Host \$host;
        proxy_set_header X-Forwarded-Port \$server_port;

        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection \"upgrade\";

        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;

        proxy_buffering on;
        proxy_buffer_size 128k;
        proxy_buffers 4 256k;
        proxy_busy_buffers_size 256k;
    }

    location /static/ {
        alias /opt/trendvision/static/;
        expires 1y;
        add_header Cache-Control \"public, immutable\";
        access_log off;
    }

    location /api/ {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        add_header Cache-Control \"no-cache, no-store, must-revalidate\";
        add_header Pragma \"no-cache\";
        add_header Expires \"0\";
    }
}
EOF"
log_success "nginx site file created"

log_step "Step 13: Enable nginx site"
run_remote "sudo ln -sf /etc/nginx/sites-available/trendvision2004.com /etc/nginx/sites-enabled/trendvision2004.com && sudo rm -f /etc/nginx/sites-enabled/default"
log_success "nginx site enabled"

log_step "Step 14: Test nginx configuration and reload"
run_remote "sudo nginx -t && sudo systemctl reload nginx"
log_success "nginx configuration reloaded"

log_step "Step 15: Apply ownership and permissions"
run_remote "sudo chown -R trendvision:trendvision /opt/trendvision && sudo chmod +x /opt/trendvision/*.py"
log_success "Ownership and permissions updated"

log_step "Step 16: Configure log rotation"
run_remote "sudo tee /etc/logrotate.d/trendvision << 'EOF'
/opt/trendvision/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 trendvision trendvision
    postrotate
        systemctl reload trendvision-web
        systemctl reload trendvision-pipeline
    endscript
}
EOF"
log_success "Logrotate configuration created"

log_step "Step 17: Request SSL certificates"
run_remote "sudo certbot --nginx -d $DOMAIN -d $ALT_DOMAIN --email admin@trendvision2004.com --agree-tos --non-interactive --redirect"
log_success "SSL certificates installed"

log_step "Step 18: Enable and start services"
run_remote "sudo systemctl daemon-reload && sudo systemctl enable trendvision-web trendvision-pipeline && sudo systemctl start trendvision-web trendvision-pipeline"
log_success "Services enabled and started"

log_step "Step 19: Verify service status"
run_remote "sudo systemctl status trendvision-web --no-pager && sudo systemctl status trendvision-pipeline --no-pager && sudo systemctl status nginx --no-pager"
log_success "Services are running"

log_step "Step 20: Review recent pipeline logs"
run_remote "sudo journalctl -u trendvision-pipeline --since '5 minutes ago' --no-pager"
log_success "Recent pipeline logs displayed"

log_step "Step 21: Create admin helper script"
run_remote "cat > ~/trendvision-admin.sh << 'EOF'
#!/bin/bash
echo 'TrendVision Admin Commands'
echo '1. Check web status: sudo systemctl status trendvision-web'
echo '2. Check pipeline status: sudo systemctl status trendvision-pipeline'
echo '3. View web logs: sudo journalctl -u trendvision-web -f'
echo '4. View pipeline logs: sudo journalctl -u trendvision-pipeline -f'
echo '5. Restart web: sudo systemctl restart trendvision-web'
echo '6. Restart pipeline: sudo systemctl restart trendvision-pipeline'
echo '7. Admin panel: https://$DOMAIN/admin/login'
echo '8. Credentials: dsar / dsar'
EOF
chmod +x ~/trendvision-admin.sh"
log_success "Admin helper script created"

log_step "Step 22: Test HTTP to HTTPS redirect"
HTTP_STATUS=$(curl -I -s "http://$DOMAIN" | head -n 1 || true)
log_info "HTTP response: $HTTP_STATUS"

log_step "Step 23: Test HTTPS response"
HTTPS_STATUS=$(curl -I -s "https://$DOMAIN" | head -n 1 || true)
log_info "HTTPS response: $HTTPS_STATUS"

log_step "Step 24: Test admin login page"
ADMIN_STATUS=$(curl -I -s "https://$DOMAIN/admin/login" | head -n 1 || true)
log_info "Admin response: $ADMIN_STATUS"

log_step "Deployment summary"
log_info "VM: $VM_NAME ($VM_IP) in $ZONE"
log_info "Site: https://$DOMAIN"
log_info "Admin: https://$DOMAIN/admin/login"
log_info "Credentials: dsar / dsar"
log_info "Next: update the daily access token, fetch instruments, extract options, and monitor the pipeline."

log_success "TrendVision deployment completed"
