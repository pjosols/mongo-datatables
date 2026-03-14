#!/usr/bin/env python3
"""Comprehensive quality validation for mongo-datatables improvements."""

import subprocess
import sys
import time
import json
from pathlib import Path

def run_command(cmd, cwd=None):
    """Run command and return result."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    return result.returncode == 0, result.stdout, result.stderr

def test_mongo_datatables():
    """Test mongo-datatables library."""
    print("=== Testing mongo-datatables library ===")
    
    # Activate venv and run tests
    success, stdout, stderr = run_command(
        "source venv/bin/activate && python -m pytest tests/ -v --tb=short",
        cwd="/Users/polsen/Projects/mongo-datatables"
    )
    
    if success:
        # Extract test count
        lines = stdout.split('\n')
        for line in lines:
            if 'passed' in line and 'in' in line:
                print(f"✅ {line.strip()}")
                break
    else:
        print(f"❌ Tests failed: {stderr}")
    
    return success

def test_coverage():
    """Test coverage."""
    print("\n=== Testing coverage ===")
    
    success, stdout, stderr = run_command(
        "source venv/bin/activate && python -m pytest tests/ --cov=mongo_datatables --cov-report=term-missing",
        cwd="/Users/polsen/Projects/mongo-datatables"
    )
    
    if success:
        lines = stdout.split('\n')
        for line in lines:
            if 'TOTAL' in line and '%' in line:
                print(f"✅ Coverage: {line.strip()}")
                break
    else:
        print(f"❌ Coverage test failed: {stderr}")
    
    return success

def test_flask_demo():
    """Test Flask demo integration."""
    print("\n=== Testing Flask demo integration ===")
    
    # Install local mongo-datatables
    success, _, stderr = run_command(
        "source venv/bin/activate && pip install -e ../mongo-datatables",
        cwd="/Users/polsen/Projects/flask-demo"
    )
    
    if not success:
        print(f"❌ Failed to install local mongo-datatables: {stderr}")
        return False
    
    # Run Flask demo tests
    success, stdout, stderr = run_command(
        "source venv/bin/activate && python -m pytest test_integration_simple.py -v",
        cwd="/Users/polsen/Projects/flask-demo"
    )
    
    if success:
        print("✅ Flask demo integration tests passed")
    else:
        print(f"❌ Flask demo tests failed: {stderr}")
    
    return success

def test_django_demo():
    """Test Django demo integration."""
    print("\n=== Testing Django demo integration ===")
    
    # Install local mongo-datatables
    success, _, stderr = run_command(
        "source venv/bin/activate && pip install -e ../mongo-datatables",
        cwd="/Users/polsen/Projects/django-demo"
    )
    
    if not success:
        print(f"❌ Failed to install local mongo-datatables: {stderr}")
        return False
    
    # Run Django demo tests
    success, stdout, stderr = run_command(
        "source venv/bin/activate && python -m pytest test_integration.py -v",
        cwd="/Users/polsen/Projects/django-demo"
    )
    
    if success:
        print("✅ Django demo integration tests passed")
    else:
        print(f"❌ Django demo tests failed: {stderr}")
    
    return success

def performance_benchmark():
    """Run performance benchmarks."""
    print("\n=== Performance benchmarks ===")
    
    # Test count optimization
    success, stdout, stderr = run_command(
        "source venv/bin/activate && python -m pytest tests/test_count_optimization.py -v",
        cwd="/Users/polsen/Projects/mongo-datatables"
    )
    
    if success:
        print("✅ Count optimization tests passed")
    else:
        print(f"❌ Count optimization tests failed: {stderr}")
        return False
    
    # Test config validation performance
    success, stdout, stderr = run_command(
        "source venv/bin/activate && python -m pytest tests/test_config_validation.py::TestPerformanceValidation -v",
        cwd="/Users/polsen/Projects/mongo-datatables"
    )
    
    if success:
        print("✅ Config validation performance tests passed")
    else:
        print(f"❌ Config validation performance tests failed: {stderr}")
        return False
    
    return True

def main():
    """Run comprehensive quality validation."""
    print("🔍 Starting comprehensive quality validation for mongo-datatables")
    
    results = {
        "mongo_datatables_tests": test_mongo_datatables(),
        "coverage": test_coverage(),
        "flask_demo": test_flask_demo(),
        "django_demo": test_django_demo(),
        "performance": performance_benchmark()
    }
    
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name:25} {status}")
        if not passed:
            all_passed = False
    
    print("="*60)
    if all_passed:
        print("🎉 ALL QUALITY VALIDATIONS PASSED")
        return 0
    else:
        print("⚠️  SOME VALIDATIONS FAILED")
        return 1

if __name__ == "__main__":
    sys.exit(main())