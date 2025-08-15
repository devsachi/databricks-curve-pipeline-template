"""Configuration package for curve formation pipeline"""
from pathlib import Path

CONFIG_ROOT = Path(__file__).parent
BASE_CONFIG = CONFIG_ROOT / 'base.yaml'
DEV_CONFIG = CONFIG_ROOT / 'dev.yaml'
PROD_CONFIG = CONFIG_ROOT / 'prod.yaml'

__all__ = [
    'CONFIG_ROOT',
    'BASE_CONFIG',
    'DEV_CONFIG',
    'PROD_CONFIG'
]
