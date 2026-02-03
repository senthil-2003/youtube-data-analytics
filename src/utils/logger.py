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
        raise RuntimeWarning(
            "Wrong logging level given as input. Check configuration and try again."
        )
    return level

def get_logger(log_path: str, log_file_name: str, log_level: str, azure_connection_string: str):
    os.makedirs(log_path, exist_ok = True)
    
    logging_level = get_logging_level(log_level)
    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.propagate = False
    
    formatter = logging.Formatter(
                    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                    datefmt = "%Y-%m-%d %H:%M:%S"
    )
    
    file_handler = TimedRotatingFileHandler(
        filename = os.path.join(log_path,log_file_name),
        when = "W0",
        interval = 1,
        backupCount = 12,
        encoding = "utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging_level)
    
    # === Console handler ===
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.WARNING)
    
    azure_handler = None
    try:
        azure_handler = AzureLogHandler(connection_string = azure_connection_string)    
        azure_handler.setFormatter(formatter)
        azure_handler.setLevel(logging.INFO)
    except Exception as e:
        raise RuntimeError(f"Azure log handling cannot be created,check the connection string {e}")
    
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        if azure_handler:
            logger.addHandler(azure_handler)
        
    return logger