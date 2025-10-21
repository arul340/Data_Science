# File Handler.Py module
import pandas as pd
from pathlib import Path
from src.utils import log_action


# Mengecek apakah file harian tersedia di folder raw
def check_file_exists(file_path: Path) -> bool:
    exists = file_path.exists()
    if exists :
        log_action (f"File di temukan: {file_path.name}")
    else:
        log_action(f"File tidak ditemukan: {file_path.name}")
    return exists

# Membaca file CSV dan mengembalikannya sebagai DataFrame.
def read_csv(file_path:Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path)
        print(f"Berhasil membaca file CSV: {file_path.name}")
        return df
    except Exception as error:
        print(f"Gagal membaca file CSV: {error}")
        return pd.DataFrame()
    
#  Memindahkan file dari folder raw ke folder processed. Jika file tujuan sudah ada, akan diganti dengan versi baru.
def move_to_processed(file_path:Path, processed_dir:Path) -> bool:
    try:
        processed_dir.mkdir(parents=True, exist_ok=True)
        destination = processed_dir / file_path.name
        if destination.exists():
            destination.unlink()
            log_action(f"File lama dihapus: {destination.name}")

        file_path.rename(destination)
        log_action(f"File berhasil dipindahkan ke processed: {destination.name}")
        return True   
    except Exception as error:
        print(f"Gagal memindahkan file: {error}")
        return False

#  Menyimpan DataFrame sebagai file CSV hasil laporan.
def write_csv(file_path: Path, df: pd.DataFrame) -> bool:
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(file_path, index=False)
        print(f"File CSV berhasil disimpan di {file_path}")
        return True
    except Exception as error:
        print(f"Gagal menulis file CSV: {error}")
        return False

