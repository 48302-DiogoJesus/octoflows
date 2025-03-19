import logging
import sys

def create_logger(name: str):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        level = logging.INFO
        logger.setLevel(level)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        file_handler = logging.FileHandler("app.log")
        file_handler.setLevel(level)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger
