import logging
import sys
from datetime import datetime

def create_logger(name: str, prefix: str = "") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        level = logging.INFO
        logger.setLevel(level)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        file_handler = logging.FileHandler("app.log")
        file_handler.setLevel(level)

        class PrefixFormatter(logging.Formatter):
            def __init__(self, prefix: str = "", *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.prefix = prefix

            def format(self, record: logging.LogRecord) -> str:
                message = record.getMessage()
                if self.prefix:
                    message = f"[{self.prefix}] {message}"
                record.msg = message
                return super().format(record)

            def formatTime(self, record, datefmt=None):
                # Force UTC time
                ct = datetime.utcfromtimestamp(record.created)
                if datefmt:
                    s = ct.strftime(datefmt)
                else:
                    s = ct.strftime("%Y-%m-%d %H:%M:%S") + ",{:03d}".format(int(record.msecs))
                return s + " UTC"

        formatter = PrefixFormatter(
            prefix=prefix,
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger