#!/usr/bin/env python3
"""
Debug deployment script for TrendVision
"""

import sys
import os
import subprocess
import traceback

def check_files():
    """Check if all required files exist"""
    required_files = [
        'app.py',
        'pipeline1.py',
        'requirements.txt',
        'extracted_data.csv',
        'config/config.json',
        'MarketDataFeedV3_pb2.py'
    ]

    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path} exists")
        else:
            print(f"❌ {file_path} MISSING")

def test_imports():
    """Test Python imports"""
    try:
        print("Testing basic imports...")
        import flask
        print("✅ Flask import successful")
    except ImportError as e:
        print(f"❌ Flask import failed: {e}")

    try:
        import pandas as pd
        print("✅ Pandas import successful")
    except ImportError as e:
        print(f"❌ Pandas import failed: {e}")

def test_app_syntax():
    """Test app.py syntax"""
    try:
        print("Testing app.py syntax...")
        with open('app.py', 'r') as f:
            compile(f.read(), 'app.py', 'exec')
        print("✅ app.py syntax is valid")
    except Exception as e:
        print(f"❌ app.py syntax error: {e}")

def test_pipeline_syntax():
    """Test pipeline1.py syntax"""
    try:
        print("Testing pipeline1.py syntax...")
        with open('pipeline1.py', 'r') as f:
            compile(f.read(), 'pipeline1.py', 'exec')
        print("✅ pipeline1.py syntax is valid")
    except Exception as e:
        print(f"❌ pipeline1.py syntax error: {e}")
        return False
    return True

def test_start_app():
    """Try to start the app briefly"""
    try:
        print("Testing app startup...")
        import app
        print("✅ app.py imports successfully")
    except Exception as e:
        print(f"❌ app.py startup error: {e}")

if __name__ == "__main__":
    print("🔍 TrendVision Debug Script")
    print("=" * 50)

    check_files()
    print()
    test_imports()
    print()
    test_app_syntax()
    print()
    pipeline_ok = test_pipeline_syntax()
    print()
    test_start_app()

    print("=" * 50)
    if pipeline_ok:
        print("🎉 Pipeline syntax check passed!")
    else:
        print("❌ Pipeline has syntax errors - fix before deployment")
