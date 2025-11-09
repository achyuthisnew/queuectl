"""Configuration management for QueueCTL."""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from .utils import logger


class Config:
    """
    Manages system configuration with persistent storage.
    """
    
    DEFAULT_CONFIG = {
        "max_retries": 3,
        "backoff_base": 2.0,
        "worker_poll_interval": 1.0,
        "data_dir": ".queuectl",
    }
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to config file (default: .queuectl/config.json)
        """
        if config_path is None:
            data_dir = Path.home() / ".queuectl"
            data_dir.mkdir(exist_ok=True)
            config_path = str(data_dir / "config.json")
        
        self.config_path = config_path
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from file or create default.
        
        Returns:
            Configuration dictionary
        """
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                    # Merge with defaults for any missing keys
                    return {**self.DEFAULT_CONFIG, **config}
            except Exception as e:
                logger.error(f"Failed to load config: {e}")
                return self.DEFAULT_CONFIG.copy()
        else:
            return self.DEFAULT_CONFIG.copy()
    
    def _save_config(self) -> None:
        """Save configuration to file."""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as f:
                json.dump(self._config, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            raise
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value and persist.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        self._config[key] = value
        self._save_config()
        logger.info(f"Configuration updated: {key} = {value}")
    
    def get_all(self) -> Dict[str, Any]:
        """
        Get all configuration values.
        
        Returns:
            Complete configuration dictionary
        """
        return self._config.copy()
