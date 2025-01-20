import yaml
from pathlib import Path
import logging
from typing import Dict, Any

def load_config() -> Dict[str, Any]:
    """Load configuration from yaml file."""
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
        
    if not config:
        raise ValueError("Config file is empty")
        
    return config

def setup_logger(name: str) -> logging.Logger:
    """Setup logger with standard configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(name)

def ensure_data_dirs(config: Dict[str, Any]) -> None:
    """Ensure data directories exist."""
    Path(config['data']['raw_dir']).mkdir(parents=True, exist_ok=True)
    Path(config['data']['processed_dir']).mkdir(parents=True, exist_ok=True)

def get_data_path(config: Dict[str, Any], filename: str, processed: bool = True) -> Path:
    """Get the full path for a data file."""
    base_dir = config['data']['processed_dir'] if processed else config['data']['raw_dir']
    return Path(base_dir) / filename