import datetime as dt
from pathlib import Path
from typing import Optional
import logging
import sphereDA.data.surface as surface
from .utils import setup_logging, get_data_path

logger = logging.getLogger(__name__)

def process_surface_data(
    date: dt.datetime,
    output_dir: Optional[Path] = None
) -> bool:
    """
    Process surface data for a specific date.
    
    Args:
        date: DateTime object for data processing
        output_dir: Optional output directory path
    
    Returns:
        bool: True if processing was successful
    """
    setup_logging()
    logger.info(f"Processing data for {date}")
    
    try:
        # Setup path
        data_path = get_data_path(date)
        print(data_path)
        if output_dir:
            data_path = output_dir / data_path
            
        # Execute pipeline
        if not surface.download_rda_data(date, str(data_path)):
            logger.error("Download failed")
            return False
            
        if not surface.decode_bufr_data(str(data_path), date.hour):
            logger.error("BUFR decoding failed")
            return False
            
        if not surface.convert_bufr_to_feather(str(data_path)):
            logger.error("Feather conversion failed")
            return False
            
        logger.info(f"Successfully processed data for {date}")
        return True
        
    except Exception as e:
        logger.error(f"Error in processing pipeline: {e}")
        return False

def process_date_range(
    start_date: dt.datetime,
    end_date: dt.datetime,
    interval_hours: int = 6,
    output_dir: Optional[Path] = None
) -> None:
    """
    Process surface data for a range of dates.
    
    Args:
        start_date: Starting datetime
        end_date: Ending datetime
        interval_hours: Hours between processes (default: 6)
        output_dir: Optional output directory path
    """
    current = start_date
    while current <= end_date:
        success = process_surface_data(current, output_dir)
        if success:
            print(f"Successfully processed {current}")
        else:
            print(f"Failed to process {current}")
        current += dt.timedelta(hours=interval_hours)
