#!/bin/bash
# TrendVision Deployment Script

# Exit on any error
set -e

# --- System Update and Dependency Installation ---
echo "Updating system and installing dependencies..."
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install -y python3-pip python3-venv nginx certbot python3-certbot-nginx git

# --- Project Setup ---
PROJECT_DIR="/var/www/TrendVision"
echo "Cloning repository into $PROJECT_DIR..."
sudo git clone https://github.com/Deepakmapalakaje/TrendVision.git $PROJECT_DIR
cd $PROJECT_DIR

# --- Python Virtual Environment ---
echo "Setting up Python virtual environment..."
sudo python3 -m venv venv
source venv/bin/activate

# --- Install Python Dependencies ---
echo "Installing Python requirements..."
pip3 install -r requirements.txt
pip3 install gunicorn # Add gunicorn for production

# --- Environment Configuration ---
# Note: You must edit this file later to add your Upstox Access Token
echo "Creating .env file..."
sudo tee .env > /dev/null <<EOF
FLASK_SECRET_KEY='$(openssl rand -hex 16)'
UPSTOX_ACCESS_TOKEN='YOUR_ACCESS_TOKEN_HERE'
EOF

# --- Systemd Service Setup ---
echo "Creating systemd service file..."
sudo tee /etc/systemd/system/trendvision.service > /dev/null <<EOF
[Unit]
Description=TrendVision Flask Application
After=network.target

[Service]
User=www-data
Group=www-data
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/venv/bin/gunicorn --workers 3 --bind unix:trendvision.sock -m 007 app:app
Restart=always

[Install]
WantedBy=multi-user.target
EOF

# --- Nginx Configuration ---
echo "Configuring Nginx..."
sudo tee /etc/nginx/sites-available/trendvision > /dev/null <<EOF
server {
    listen 80;
    server_name trendvision2004.com www.trendvision2004.com;

    location / {
        include proxy_params;
        proxy_pass http://unix:$PROJECT_DIR/trendvision.sock;
    }
}
EOF

# Enable the Nginx site
sudo ln -s -f /etc/nginx/sites-available/trendvision /etc/nginx/sites-enabled/
sudo nginx -t # Test Nginx configuration

# --- Start Services & Configure SSL ---
echo "Starting services and configuring SSL..."
sudo systemctl daemon-reload
sudo systemctl start trendvision
sudo systemctl enable trendvision
sudo systemctl restart nginx

# Obtain and install SSL certificate
echo "Requesting SSL certificate from Let's Encrypt..."
sudo certbot --nginx -d trendvision2004.com -d www.trendvision2004.com --non-interactive --agree-tos -m your-email@example.com

echo "Deployment complete!"
echo "Your application should now be live at https://trendvision2004.com"
echo "IMPORTANT: Remember to edit /var/www/TrendVision/.env to add your Upstox Access Token."
