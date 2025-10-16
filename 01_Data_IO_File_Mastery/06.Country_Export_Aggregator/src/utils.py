import logging
from datetime import datetime
from pathlib import Path

def setup_logging(path_log: Path):
    # Setup file log
    path_log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=path_log,
        level=logging.INFO,
        format = "%(asctime)s - %(levelname)s - %(message)s",
    )

    # Add handler console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logging.getLogger().addHandler(console)


def log_action (message: str) :
    logging.info(f"{datetime.now().strftime('%Y-%m-%d : %H:%M:%S')} {message}")