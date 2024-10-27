import datetime as dt
from pathlib import Path
import logging
from typing import Optional, Union

def setup_logging(level: str = "INFO") -> None:
    """Configure logging for the package"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('sphereda.log')
        ]
    )

def get_data_path(date: dt.datetime, base_path: Optional[str] = None) -> Path:
    """
    Generate standardized path for data storage
    
    Args:
        date: DateTime object
        base_path: Optional base directory
    
    Returns:
        Path object for data directory
    """
    folder_name = date.strftime('sup_%Y%m%d%H')
    if base_path:
        return Path(base_path) / folder_name
    return Path(folder_name)

