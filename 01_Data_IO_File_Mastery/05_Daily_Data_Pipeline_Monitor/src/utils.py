# Utils.Py module
import logging 
from datetime import datetime
from pathlib import Path


# Menyiapkan sistem logging untuk mencatat semua aktivitas pipeline.
def setup_logging(log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",  
    )

    # # Tambahkan console handler agar log juga muncul di terminal
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logging.getLogger().addHandler(console)

# Mencatat pesan ke log file dan menampilkannya di terminal.
def log_action(message: str) :
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"[{timestamp}] - {message}")
    print(f"[{timestamp}] - {message}")

# Mengembalikan tanggal hari ini dalam format YYYY-MM-DD.
def get_today_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")