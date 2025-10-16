from pathlib import Path
import pandas as pd
from src.file_handler import write_csv, write_excel
from src.merge import merge_excels
from src.utils import setup_logging, log_action

# ------------------------------------------------------------  

def main():
    # === Page Setup ===
    BASE_DIR = Path(__file__).resolve().parent
    RAW_DIR = BASE_DIR / "data" / "raw"
    PROCESSED_DIR = BASE_DIR / "data" / "processed"
    LOG_PATH = BASE_DIR / "logs" / "app.log"

    setup_logging(LOG_PATH)
    log_action("Memulai proses integrasi data...")

    # === Gabungkan File ===
    combined_df = merge_excels(RAW_DIR)
    if combined_df.empty:
        log_action("Tidak ada data yang digabungkan. ")
        return
    
    # === Simpan Hasil Utama ===
    master_path = PROCESSED_DIR / "master_data.xlsx"
    csv_path = PROCESSED_DIR / "master_data.csv"
    write_excel(master_path, combined_df)
    write_csv(csv_path, combined_df)

    # Buat Summary sederhana
    summary = combined_df.groupby("category")[["price", "quantity"]].sum().reset_index()

    # === Tambahkan summary sebagai sheet baru ===
    with pd.ExcelWriter(master_path, engine="openpyxl", mode="a") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)

    log_action("Integrasi Excel selesai tanpa error")
    print("Semua Proses Selesai")

if __name__ == "__main__":
    main()

