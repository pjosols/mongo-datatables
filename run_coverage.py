#!/usr/bin/env python
import subprocess
import sys


def run_tests_with_coverage():
    """Run tests with coverage using pytest"""
    print("\n=== Running Tests with Coverage Analysis ===\n")

    # Run pytest with coverage
    result = subprocess.run([
        "python", "-m", "pytest",
        "--cov=mongo_datatables", "tests/",
        "--cov-report=term",
        "--cov-report=html",
        "--cov-report=xml"
    ], check=False)

    if result.returncode == 0:
        print("\nTests completed successfully!")
    else:
        print("\nSome tests failed.")

    return result.returncode == 0


if __name__ == "__main__":
    result = run_tests_with_coverage()
    sys.exit(0 if result else 1)