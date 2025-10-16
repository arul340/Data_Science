import pandas as pd
from pathlib import Path
from src.file_handler import write_csv, write_excel
from src.aggregator import merge_excel, calculate_total_value, get_top_products
from src.utils import log_action, setup_logging


def main():
    # === Setup Page ===
    BASE_DIR = Path(__file__).resolve().parent
    RAW_DIR = BASE_DIR / "data" / "raw"
    PROCESSED_DIR = BASE_DIR / "data" / "processed"
    LOG_PATH = BASE_DIR / "logs" / "app.log"

    setup_logging(LOG_PATH)
    log_action("Memulai proses penggabungan data...")

    # === Gabungkan semua file Excel ===
    combined_df = merge_excel(RAW_DIR)
    if combined_df.empty:
        log_action(LOG_PATH, "Tidak ada data yang digabungkan.")
        return

    # === Simpan hasil gabungan utama ===
    result_path = PROCESSED_DIR / "combined_data.xlsx"
    csv_path = PROCESSED_DIR / "combined_data.csv"

    write_csv(csv_path, combined_df)
    write_excel(result_path, combined_df)

    # === Hitung summary dan top products ===
    summary = calculate_total_value(combined_df)
    top_products = get_top_products(summary)

    # === Tambahkan kedua hasil ke file Excel ===
    with pd.ExcelWriter(result_path, engine="openpyxl", mode="a") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        top_products.to_excel(writer, sheet_name="top_products", index=False)

    log_action("Proses penggabungan data selesai tanpa error")
    print("✅ Proses penggabungan data selesai tanpa error")


if __name__ == "__main__":
    main()
