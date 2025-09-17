#!/bin/bash

# ==========================================
# DOMAIN & SSL SETUP FOR TRENDVISION2004.COM
# ==========================================

set -e

DOMAIN="trendvision2004.com"
VM_IP="34.180.14.162"
EMAIL="admin@trendvision2004.com"  # Change this to your actual email

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🌐 Setting up SSL for $DOMAIN...${NC}"

# Step 1: Check DNS propagation
echo -e "${YELLOW}1️⃣ Waiting for DNS propagation...${NC}"
echo "Please ensure your domain DNS settings point to: $VM_IP"
echo ""
echo "DNS Changes to make:"
echo "Type: A"
echo "Name: @ (or trendvision2004.com)"
echo "Value: $VM_IP"
echo ""
echo "Type: CNAME"
echo "Name: www"
echo "Value: @ (or trendvision2004.com)"
echo ""

read -p "Press Enter after DNS changes are made and propagated..."

# Test DNS resolution
echo -e "${BLUE}Testing DNS resolution...${NC}"
if nslookup $DOMAIN 8.8.8.8 | grep -q $VM_IP; then
    echo -e "${GREEN}✅ DNS resolution successful${NC}"
else
    echo -e "${RED}❌ DNS not yet propagated. Please wait and try again${NC}"
    exit 1
fi

# Step 2: Install SSL certificate
echo -e "${YELLOW}2️⃣ Installing SSL certificate...${NC}"

if ! command -v certbot &> /dev/null; then
    echo -e "${RED}❌ Certbot not found. Installing...${NC}"
    sudo apt install -y certbot python3-certbot-nginx
fi

# Stop nginx temporarily for certbot challenge
sudo systemctl stop nginx

# Run certbot
sudo certbot certonly --standalone \
    --email "$EMAIL" \
    --agree-tos \
    --no-eff-email \
    --domain "$DOMAIN" \
    --domain "www.$DOMAIN"

# Restart nginx
sudo systemctl start nginx

echo -e "${GREEN}✅ SSL certificate installed${NC}"

# Step 3: Configure SSL in Nginx
echo -e "${YELLOW}3️⃣ Configuring SSL in Nginx...${NC}"

# Create SSL-enabled Nginx config
sudo tee /etc/nginx/sites-available/trendvision2004.com.ssl > /dev/null <<EOF
# HTTP to HTTPS redirect
server {
    listen 80;
    server_name trendvision2004.com www.trendvision2004.com;
    return 301 https://\$server_name\$request_uri;
}

# HTTPS configuration
server {
    listen 443 ssl http2;
    server_name trendvision2004.com www.trendvision2004.com;

    # SSL Configuration
    ssl_certificate /etc/letsencrypt/live/$DOMAIN/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/$DOMAIN/privkey.pem;

    # SSL Security Settings
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;

    # HSTS
    add_header Strict-Transport-Security "max-age=63072000" always;

    # Main application proxy
    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header X-Forwarded-Host \$server_name;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";

        # Timeout settings
        proxy_connect_timeout 75s;
        proxy_send_timeout 75s;
        proxy_read_timeout 75s;
    }

    # Static files with caching
    location /static/ {
        alias /home/trading_app/static/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    # Health check
    location /health {
        access_log off;
        return 200 "healthy\n";
        add_header Content-Type text/plain;
    }

    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
}
EOF

# Enable new config and reload
sudo ln -sf /etc/nginx/sites-available/trendvision2004.com.ssl /etc/nginx/sites-enabled/trendvision2004.com
sudo rm -f /etc/nginx/sites-enabled/trendvision2004.com
sudo systemctl reload nginx

echo -e "${GREEN}✅ SSL configuration complete${NC}"

# Step 4: Add SSL renewal to crontab
echo -e "${YELLOW}4️⃣ Setting up SSL auto-renewal...${NC}"

# Add to crontab if not already there
if ! crontab -l | grep -q certbot; then
    (crontab -l ; echo "0 0 * * * /usr/bin/certbot renew --quiet --post-hook 'systemctl reload nginx'") | crontab -
    echo -e "${GREEN}✅ SSL renewal scheduled${NC}"
else
    echo -e "${GREEN}✅ SSL renewal already scheduled${NC}"
fi

# Step 5: Test SSL
echo -e "${YELLOW}5️⃣ Testing SSL configuration...${NC}"

if curl -f -s https://$DOMAIN/health > /dev/null; then
    echo -e "${GREEN}✅ SSL working correctly${NC}"

    # Show certificate info
    echo -e "${BLUE}Certificate Information:${NC}"
    curl -vI https://$DOMAIN 2>&1 | grep -E "(server certificate|subject:|issuer:|expire date)"
else
    echo -e "${RED}❌ SSL test failed${NC}"
    echo -e "${YELLOW}⚠️  Check nginx logs: sudo journalctl -u nginx${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}===========================================${NC}"
echo -e "${GREEN}🎉 SSL SETUP COMPLETED SUCCESSFULLY!${NC}"
echo -e "${GREEN}===========================================${NC}"
echo ""
echo -e "${BLUE}📋 Your domain is now live at:${NC}"
echo -e "${GREEN}   https://$DOMAIN${NC}"
echo -e "${GREEN}   https://www.$DOMAIN${NC}"
echo ""
echo -e "${BLUE}🔒 SSL Certificate Details:${NC}"
echo -e "   - Issued by: Let's Encrypt"
echo -e "   - Auto-renews every 90 days"
echo -e "   - Ciphers: Modern TLS 1.2/1.3"
echo ""
echo -e "${BLUE}🛠️  Useful Commands:${NC}"
echo -e "   Certificate info:   openssl s_client -connect $DOMAIN:443 2>/dev/null | openssl x509 -noout -dates -subject -issuer"
echo -e "   Renewal test:       sudo certbot renew --dry-run"
echo -e "   Logs:              sudo journalctl -u nginx"
echo -e "   Restart services:  sudo systemctl restart trading-app nginx"
