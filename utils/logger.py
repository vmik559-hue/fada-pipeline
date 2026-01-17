"""
Central Logging System for FADA ETL Pipeline
Provides consistent logging across all modules with file and console output.
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Global logger instance
_logger = None


def setup_logger(name: str = 'fada_pipeline', log_file: Path = None, level: str = 'INFO') -> logging.Logger:
    """
    Set up and return a configured logger.
    
    Args:
        name: Logger name
        log_file: Path to log file (optional)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    global _logger
    
    if _logger is not None:
        return _logger
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (if log_file provided)
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    _logger = logger
    return logger


def get_logger() -> logging.Logger:
    """Get the global logger instance, creating it if necessary."""
    global _logger
    if _logger is None:
        # Import config here to avoid circular imports
        try:
            from config import LOG_CONFIG
            _logger = setup_logger(
                log_file=LOG_CONFIG.get('log_file'),
                level=LOG_CONFIG.get('log_level', 'INFO')
            )
        except ImportError:
            _logger = setup_logger()
    return _logger


class PipelineLogger:
    """
    Context manager for logging pipeline operations with automatic timing.
    
    Usage:
        with PipelineLogger("Downloading PDFs") as log:
            # do work
            log.info("Downloaded file X")
    """
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.logger = get_logger()
        self.start_time = None
    
    def __enter__(self):
        import time
        self.start_time = time.time()
        self.logger.info(f"Started: {self.operation_name}")
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        elapsed = time.time() - self.start_time
        
        if exc_type is not None:
            self.logger.error(f"Failed: {self.operation_name} - {exc_val} ({elapsed:.2f}s)")
            return False
        
        self.logger.info(f"Completed: {self.operation_name} ({elapsed:.2f}s)")
        return True
