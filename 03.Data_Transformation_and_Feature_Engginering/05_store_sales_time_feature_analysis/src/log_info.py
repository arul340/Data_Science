import logging
from pathlib import Path
from datetime import datetime
import json

def setup_logger(config_path:str = "config/settings.json" ) -> logging.Logger:
    # Setup a standardized logger with timestamp, level, and module name. Reads log file path and log level from settings.json.

    # === Load Config ===
    try:
        with open(config_path, "r") as file:
            config = json.load(file)
    except:
        raise FileNotFoundError(f"Config file not found at: {config_path}")
    
    log_file_path = Path(config["paths"]["logging"]["log_file"])
    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Create Logger
    logger = logging.getLogger("Store Sales Analysis Pipeline")
    logger.setLevel(config.get("runtime", {}).get("log_level", "INFO"))

    # Prevent duplicate handlers if function is called multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    # === Formatter ===
    log_format = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)

    # === File Handler ===
    file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # === Console Handler === (optional for dev use)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info(f"Logger initialized at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Logging to file: {log_file_path}")

    return logger

def log_info(logger: logging.Logger, message:str) -> None:
    # Log info message with standardized format.
    logger.info(message)

def log_error (logger: logging.Logger, message:str, exc:Exception = None) -> None:
    # Log error message and optionally include exception details.
    if exc:
        log_error(f"{message} | Exception: {str(exc)}", exc_info=True)
    else:
        logger.error(message)

def log_stage (logger: logging.Logger, stage_name:str) -> None:
    line = "=" * 50
    logger.info(f"\n{line}\n>>> Starting Stage: {stage_name}\n{line}")

def log_success (logger: logging.Logger, stage_name: str) -> None:
    # Helper for marking stage completion.
    logger.info(f"Stage '{stage_name}'  completed successfully!\n")


if __name__ == "__main__":
    # Example usage (for quick test)
    logger = setup_logger()
    log_stage(logger, "Data Loading")
    log_info(logger, "Raw data successfully loaded.")
    log_success(logger, "Data Loading")
    
