import logging
import sys
import os

if os.getenv("LOGS") == "0":
    print("Logging is disabled. Use LOGS=1 environment variable to enable it.")
    logging.disable(logging.CRITICAL)

def create_logger(name: str, prefix: str = "") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        level = logging.INFO
        logger.setLevel(level)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        file_handler = logging.FileHandler("app.log")
        file_handler.setLevel(level)

        # Custom formatter that includes the prefix (if provided)
        class PrefixFormatter(logging.Formatter):
            def __init__(self, prefix: str = "", *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.prefix = prefix

            def format(self, record: logging.LogRecord) -> str:
                # Add prefix in brackets if it exists
                message = record.getMessage()
                if self.prefix:
                    message = f"[{self.prefix}] {message}"
                record.msg = message
                return super().format(record)

        formatter = PrefixFormatter(
            prefix=prefix,
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger