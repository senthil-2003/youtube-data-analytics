import logging
from logging.handlers import TimedRotatingFileHandler
import os
from opencensus.ext.azure.log_exporter import AzureLogHandler

def get_logging_level(log_level: str):
    """Map string log levels to logging module constants."""
    level_map = {
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "CRITICAL": logging.CRITICAL,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "NOTSET": logging.NOTSET,
    }
    level = level_map.get(log_level.strip().upper())
    if level is None:
        raise ValueError(
            "Wrong logging level given as input. Check configuration and try again."
        )
    return level

def setup_logger(root_logger_name: str, log_path: str, log_file_name: str, log_level: str, azure_connection_string: str) -> logging.Logger:
    """
    Configures the application root logger with file, console, and Azure handlers.
    Should be called ONCE at application startup.
    """
    os.makedirs(log_path, exist_ok=True)
    
    logging_level = get_logging_level(log_level)
    
    logger = logging.getLogger(root_logger_name)
    
    # Prevent duplicate handlers on repeated calls
    if logger.handlers:
        return logger
    
    logger.setLevel(logging_level)
    logger.propagate = False
    
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # === File handler ===
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_path, log_file_name),
        when="W0",
        interval=1,
        backupCount=12,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging_level)
    
    # === Console handler ===
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.WARNING)
    
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    # === Azure handler ===
    try:
        azure_handler = AzureLogHandler(connection_string=azure_connection_string)
        azure_handler.setFormatter(formatter)
        azure_handler.setLevel(logging.INFO)
        logger.addHandler(azure_handler)
    except Exception as e:
        logger.warning(f"Azure log handler could not be created: {e}")
    
    # Suppress noisy library loggers
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    
    return logger

def get_logger(root_logger_name: str, module_name: str) -> logging.Logger:
    """
    Returns a child logger under the application logger.
    Call this from ANY script to get a properly configured logger.
    
    Usage: logger = get_logger(__name__)
    """
    return logging.getLogger(f"{root_logger_name}.{module_name}")