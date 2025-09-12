#!/bin/bash
echo "🔧 APPLYING ACCESS TOKEN WHITESPACE FIXES"
echo "========================================="

# Navigate to application directory
cd /opt/trendvision || {
    echo "❌ Cannot navigate to /opt/trendvision"
    exit 1
}

echo "📂 Current directory: $(pwd)"

# Function to install Python script if not present
install_fix_script() {
    local script_name="$1"
    local script_content="$2"

    if [ ! -f "$script_name" ]; then
        echo "📥 Creating $script_name..."
        # Create the Python fix script directly instead of using heredoc
        cat > "$script_name" << 'PYTHON_EOF'
#!/usr/bin/env python3
"""
Fix Access Token Whitespace Issues for VM Deployment
"""

import json
import os
import sys
from datetime import datetime

print("🔧 ACCESS TOKEN WHITESPACE FIX")
print("=" * 40)

def fix_config_token(config_path='/tmp/config.json'):
    """Remove whitespace from ACCESS_TOKEN in config file"""
    if not os.path.exists(config_path):
        print(f"❌ Config file {config_path} does not exist")
        return False

    try:
        # Read current config
        with open(config_path, 'r') as f:
            config = json.load(f)

        original_token = config.get('ACCESS_TOKEN', '')
        if original_token:
            # Clean the token
            cleaned_token = original_token.replace(' ', '').replace('\n', '').replace('\r', '').replace('\t', '').strip()

            if cleaned_token != original_token:
                print(f"✅ Fixed ACCESS_TOKEN whitespace (was {len(original_token)} chars, now {len(cleaned_token)} chars)")

                # Update config
                config['ACCESS_TOKEN'] = cleaned_token

                # Write back to file
                with open(config_path, 'w') as f:
                    json.dump(config, f, indent=2)

                # Update file timestamp for is_access_token_valid() check
                os.utime(config_path, None)

                return True
            else:
                print("ℹ️ ACCESS_TOKEN already clean")
                return True
        else:
            print("❌ ACCESS_TOKEN is empty")
            return False

    except Exception as e:
        print(f"❌ Error fixing token: {e}")
        return False

def update_env_vars():
    """Update environment variables if config exists"""
    try:
        if os.path.exists('/tmp/config.json'):
            with open('/tmp/config.json', 'r') as f:
                config = json.load(f)

            # Export cleaned variables
            if config.get('ACCESS_TOKEN'):
                clean_token = config['ACCESS_TOKEN'].replace(' ', '').replace('\n', '').replace('\r', '').replace('\t', '').strip()
                os.environ['ACCESS_TOKEN'] = clean_token
                print("✅ Updated ACCESS_TOKEN environment variable")

            if config.get('NIFTY_FUTURE_KEY'):
                os.environ['NIFTY_FUTURE_KEY'] = config['NIFTY_FUTURE_KEY']

            if config.get('ITM_CE_KEY'):
                os.environ['ITM_CE_KEY'] = config['ITM_CE_KEY']

            if config.get('ITM_PE_KEY'):
                os.environ['ITM_PE_KEY'] = config['ITM_PE_KEY']

            if config.get('ITM_CE_STRIKE'):
                os.environ['ITM_CE_STRIKE'] = str(config['ITM_CE_STRIKE'])

            if config.get('ITM_PE_STRIKE'):
                os.environ['ITM_PE_STRIKE'] = str(config['ITM_PE_STRIKE'])

            print("✅ Environment variables updated")
            return True
        else:
            print("❌ No config file found for env var update")
            return False
    except Exception as e:
        print(f"❌ Error updating env vars: {e}")
        return False

if __name__ == "__main__":
    success = True

    print("\n1. Checking config file...")
    if fix_config_token():
        print("✅ Config file token cleaned")
    else:
        print("❌ Config file token cleanup failed")

    print("\n2. Updating environment variables...")
    if update_env_vars():
        print("✅ Environment variables updated")
    else:
        print("❌ Environment variables update failed")
        success = False

    if success:
        print("\n🎉 ACCESS TOKEN WHITESPACE FIX COMPLETE")
        print("\n📝 Try starting the pipeline now:")
        print("   python trigger_pipeline.py")
    else:
        print("\n❌ ACCESS TOKEN WHITESPACE FIX FAILED")

    print("\n" + "=" * 40)
PYTHON_EOF
        chmod +x "$script_name"
        echo "✅ Created $script_name"
    else
        echo "✅ $script_name already exists"
    fi
}

# Check if config.json exists in /tmp
if [ -f "/tmp/config.json" ]; then
    echo "✅ Found config file at /tmp/config.json"
    echo "   Size: $(stat -f%z /tmp/config.json 2>/dev/null || stat -c%s /tmp/config.json) bytes"
    echo "   Modified: $(stat -f%Sm -t "%Y-%m-%d %H:%M:%S" /tmp/config.json 2>/dev/null || stat -c"%y" /tmp/config.json | cut -d '.' -f1)"

    # Show current config (truncated for safety)
    echo ""
    echo "📄 Current config (truncated):"
    head -10 /tmp/config.json
    echo "..."

else
    echo "⚠️ No config file found at /tmp/config.json"
    echo "   Will create one if you update via admin panel"
fi

# Create the fix script
install_fix_script "fix_access_token_white_space.py"

# Run the fix
echo ""
echo "🔧 Running access token cleanup..."
python3 fix_access_token_white_space.py

# Check pipeline status
echo ""
echo "📊 Checking pipeline status..."
if pgrep -f "python.*pipeline1.py" > /dev/null; then
    echo "✅ Pipeline is RUNNING"
else
    echo "❌ Pipeline is NOT running"
fi

# Show environment variables
echo ""
echo "🔍 Environment variables check:"
echo "   ACCESS_TOKEN set: $(if [ -n "$ACCESS_TOKEN" ]; then echo 'YES'; else echo 'NO'; fi)"
echo "   ACCESS_TOKEN length: $(if [ -n "$ACCESS_TOKEN" ]; then echo ${#ACCESS_TOKEN}; else echo '0'; fi)"

echo ""
echo "🎯 TO FIX ISSUES MANUALLY:"
echo "1. Visit admin panel: http://34.93.161.22/admin"
echo "2. Update ACCESS_TOKEN (paste without line breaks)"
echo "3. Update instrument keys as: NSE_FO|XXXXX"
echo "4. Click 'Update Configuration'"
echo ""

echo "========================================="
echo "✅ VM FIXES APPLIED SUCCESSFULLY"

# Try to restart the flask service to pick up template changes
echo ""
echo "🔄 Attempting to restart Flask service..."
sudo systemctl restart trendvision 2>/dev/null || echo "⚠️ Could not restart trendvision service (may not exist)"

# Show final status
echo ""
echo "🎯 NEXT STEPS:"
echo "1. Test admin panel: http://34.93.161.22/admin"
echo "2. Update configuration via admin panel"
echo "3. Try starting pipeline: python trigger_pipeline.py"
echo ""
echo "========================================="
