import logging
import sys

def create_logger(name: str):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # file_handler = logging.FileHandler("app.log")
        # file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)
        # file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        # logger.addHandler(file_handler)
    
    return logger
