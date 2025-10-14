# Utils.Py module
from pathlib import Path
from datetime import datetime
import logging, json, csv

def to_int_safe(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default
    
def setup_logging(log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def log_action(log_path: Path, message: str):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y - %m - %d")
    with open(log_path, "a", encoding="utf-8") as file:
        file.write(f"[{ts}] {message} \n")