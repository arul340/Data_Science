import logging

from pathlib import Path
from datetime import datetime
import json

def setup_logger(config_path: str = "config/settings.json") -> logging.Logger:
    # === Load Config ===
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            config = json.load(file)

    except Exception as error:
        raise KeyError (f"Config file not found at: {config_path} Error: {error}")
    
    log_file_config = Path(config["path"]["logs"]["log_file"])
    log_file_config.parent.mkdir(parents=True, exist_ok=True)

    # -- Create Logger ---
    logger = logging.getLogger("Retail Pipeline")
    runtime_cfg = (
        config.get("runtime")
        or config.get("run_time")
        or {}
    )

    logger.setLevel(runtime_cfg.get("log_level", "INFO"))

    if logger.hasHandlers():
        logger.hasHandlers.clear()

    # --- Formatter ---
    log_format = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    date_format = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)

    # --- Console Handler ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    # --- File Handler ---
    file_handler = logging.FileHandler(log_file_config)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    return logger

def log_info(logging:logging.Logger, message: str) -> None:
    logging.info(message)

def log_error(logging:logging.Logger, message: str, exc: Exception = None) -> None:
    if exc:
        logging.error(f"{message} | Exception: {str(exc)}", exc_info=True)
    else:
        logging.error(message)

def log_warning(logging: logging.Logger, message: str) -> None:
    logging.warning(message)

def log_stage(logging: logging.Logger, stage_name: str) -> None:
    line = "=" * 50
    logging.info(f"\n{line}\n>>> Starting Stage: {stage_name}\n{line}")

def log_success(logging: logging.Logger, stage_name: str) -> None:
    logging.info(f"Stage '{stage_name}' completed successfully!\n")

if __name__ == "__main__":
    logger = setup_logger()
    log_info(logger, "Test Log Info Message")
    log_error(logger, "Test Log Error Message")
    log_warning(logger, "Test Log Warning Message")
    log_stage(logger, "Test Log Stage")
    log_stage(logger, "Test Log Stage")

