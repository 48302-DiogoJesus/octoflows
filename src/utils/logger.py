import logging
import sys
import os

logs_env = os.getenv("LOGS")
logs_env = logs_env.strip() if logs_env else None
if logs_env is None or logs_env == "1":
    print("Logging for information and more important. Use LOGS=0 to just print warnings")
elif logs_env == "0":
    print("Logging only for warnings. Use LOGS=1 to also print information logs")
else:
    print(f"Invalid LOGS value (logs_env={logs_env}). Use LOGS=0 to disable logging, LOGS=1 to enable warnings and information, or LOGS=2 to enable warnings, information and debug")

def create_logger(name: str, prefix: str = "") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        level = logging.INFO
        if logs_env == "0":
            level = logging.WARNING

        logger.setLevel(level)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        file_handler = logging.FileHandler("app.log")
        # Log everything to file
        file_handler.setLevel(logging.INFO)

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
        logger.addHandler(console_handler)

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger