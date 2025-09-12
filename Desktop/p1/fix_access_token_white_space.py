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
