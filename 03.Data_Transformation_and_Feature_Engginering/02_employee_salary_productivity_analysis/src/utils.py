import logging
from typing import Final

def setup_logging(log_path: str) -> logging.Logger:
    LOG_LEVEL: Final = logging.INFO

    logging.basicConfig(
        filename=log_path,
        level=LOG_LEVEL,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    console = logging.StreamHandler()
    console.setLevel(LOG_LEVEL)
    console.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logging.getLogger().addHandler(console)

    return logging.getLogger()