#!/usr/bin/env python3

"""
Test Runner Script
Runs the test suite with coverage reporting
"""

import os
import sys
import unittest
import coverage
from datetime import datetime
import logging
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_tests():
    """Run the test suite with coverage reporting"""
    # Start coverage
    cov = coverage.Coverage(
        branch=True,
        source=['scripts'],
        omit=[
            '*/test_*.py',
            '*/__init__.py',
            '*/run_tests.py'
        ]
    )
    cov.start()
    
    # Discover and run tests
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('scripts', pattern='test_*.py')
    
    # Run tests
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)
    
    # Stop coverage
    cov.stop()
    cov.save()
    
    # Generate coverage report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir = project_root / 'reports' / 'coverage'
    report_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate HTML report
    cov.html_report(directory=str(report_dir / f'html_{timestamp}'))
    
    # Generate console report
    logger.info("\nCoverage Report:")
    cov.report()
    
    # Save coverage data
    cov.save()
    
    # Return test result
    return result.wasSuccessful()

if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1) 