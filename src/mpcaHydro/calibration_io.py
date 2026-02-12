# -*- coding: utf-8 -*-
"""
Calibration Configuration File I/O Module

This module handles loading and saving calibration configurations from/to files.
Supports YAML, JSON, and TOML formats.

This module is separate from the data classes to allow for independent development
and to be worked on later once the data class structure is established.
"""

from pathlib import Path
from typing import Union
import json

from mpcaHydro.calibration_dataclasses import CalibrationConfig

# Check for optional dependencies
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    import tomllib  # Python 3.11+
    TOML_READ_AVAILABLE = True
except ImportError:
    try:
        import tomli as tomllib  # Fallback for Python < 3.11
        TOML_READ_AVAILABLE = True
    except ImportError:
        TOML_READ_AVAILABLE = False

try:
    import tomli_w
    TOML_WRITE_AVAILABLE = True
except ImportError:
    TOML_WRITE_AVAILABLE = False


def load_config(filepath: Union[str, Path]) -> CalibrationConfig:
    """
    Load calibration configuration from a file.
    
    Supports YAML, JSON, and TOML formats based on file extension.
    
    Args:
        filepath: Path to the configuration file
        
    Returns:
        CalibrationConfig object
        
    Raises:
        ValueError: If file format is not supported
        FileNotFoundError: If file does not exist
        ImportError: If required library for file format is not installed
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"Configuration file not found: {filepath}")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    suffix = filepath.suffix.lower()
    
    if suffix in ['.yaml', '.yml']:
        if not YAML_AVAILABLE:
            raise ImportError(
                "PyYAML is required to load YAML configuration files. "
                "Install it with: pip install pyyaml"
            )
        data = yaml.safe_load(content)
    elif suffix == '.json':
        data = json.loads(content)
    elif suffix == '.toml':
        if not TOML_READ_AVAILABLE:
            raise ImportError(
                "tomllib (Python 3.11+) or tomli is required to load TOML configuration files. "
                "Install tomli with: pip install tomli"
            )
        # TOML requires binary mode for tomllib
        with open(filepath, 'rb') as f:
            data = tomllib.load(f)
    else:
        raise ValueError(
            f"Unsupported configuration file format: {suffix}. "
            "Supported formats: .yaml, .yml, .json, .toml"
        )
    
    return CalibrationConfig.from_dict(data)


def save_config(config: CalibrationConfig, filepath: Union[str, Path]) -> None:
    """
    Save calibration configuration to a file.
    
    Supports YAML, JSON, and TOML formats based on file extension.
    
    Args:
        config: CalibrationConfig object to save
        filepath: Path to save the configuration file
        
    Raises:
        ValueError: If file format is not supported
        ImportError: If required library for file format is not installed
    """
    filepath = Path(filepath)
    data = config.to_dict()
    
    # Ensure parent directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    suffix = filepath.suffix.lower()
    
    if suffix in ['.yaml', '.yml']:
        if not YAML_AVAILABLE:
            raise ImportError(
                "PyYAML is required to save YAML configuration files. "
                "Install it with: pip install pyyaml"
            )
        with open(filepath, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    elif suffix == '.json':
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    elif suffix == '.toml':
        if not TOML_WRITE_AVAILABLE:
            raise ImportError(
                "tomli-w is required to save TOML configuration files. "
                "Install it with: pip install tomli-w"
            )
        with open(filepath, 'wb') as f:
            tomli_w.dump(data, f)
    else:
        raise ValueError(
            f"Unsupported configuration file format: {suffix}. "
            "Supported formats: .yaml, .yml, .json, .toml"
        )
