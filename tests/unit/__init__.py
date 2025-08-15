"""Unit tests for curve formation pipeline"""
import os
import sys

# Add project root to Python path for test imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)
