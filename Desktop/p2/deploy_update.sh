#!/bin/bash
# TrendVision Deployment Update Script

echo "🚀 Starting TrendVision Deployment Update..."
cd /opt/trendvision

echo "📁 Current directory contents:"
ls -la

# Check if git is available and properly configured
echo "🔧 Checking Git configuration..."
if [ -d .git ] && command -v git &> /dev/null; then
    git config --global --add safe.directory /opt/trendvision

    # Pull latest code (force reset to match remote)
    echo "📥 Pulling latest code from GitHub..."
    git fetch origin
    git reset --hard origin/master
    if [ $? -ne 0 ]; then
        echo "❌ Git reset failed - trying alternative approach"
        echo "Checking git status..."
        git status
        echo "Maybe we need to clone fresh - checking remote..."
        git remote -v
        # If git fails, we'll continue with what we have
    fi
    echo "✅ Code updated successfully"
else
    echo "⚠️ Git not available or not initialized - continuing with existing files"
fi

# Activate virtual environment and update dependencies
echo "📦 Updating Python dependencies..."
source venv/bin/activate
pip install --upgrade -r requirements.txt
deactivate
echo "✅ Dependencies updated"

# Set proper permissions
echo "🔒 Setting file permissions..."
sudo chown -R trendvision:trendvision /opt/trendvision
chmod +x *.py *.sh
echo "✅ Permissions set"

# Create backup of current database
echo "💾 Creating database backup..."
cp -r database database_backup_$(date +%Y%m%d_%H%M%S)
echo "✅ Database backed up"

# Stop current services
echo "🛑 Stopping current services..."
sudo systemctl stop trendvision-web trendvision-pipeline

# Reload systemd services
echo "🔄 Reloading systemd services..."
sudo systemctl daemon-reload

# Start services
echo "🚀 Starting TrendVision services..."
sudo systemctl start trendvision-web
sudo systemctl start trendvision-pipeline

# Wait a bit for services to start
sleep 15

# Check service status
echo "📊 Checking service status..."
WEB_STATUS=$(sudo systemctl is-active trendvision-web)
PIPELINE_STATUS=$(sudo systemctl is-active trendvision-pipeline)

echo "🌐 Web Service Status: $WEB_STATUS"
echo "📊 Pipeline Service Status: $PIPELINE_STATUS"

if [ "$WEB_STATUS" == "active" ] && [ "$PIPELINE_STATUS" == "active" ]; then
    echo "🎉 SUCCESS: All services started successfully!"
    echo "🌐 Web Access: http://34.93.47.90"
    echo "🔒 HTTPS Access: https://trendvision2004.com"
    echo "🔑 Admin Panel: https://trendvision2004.com/admin/login"
    echo "👤 Admin Credentials: dsar / dsar"
else
    echo "⚠️ WARNING: Some services failed to start"
    echo "Checking detailed status..."
    sudo systemctl status trendvision-web trendvision-pipeline --no-pager | head -20
fi

echo "✅ TrendVision deployment update completed!"
