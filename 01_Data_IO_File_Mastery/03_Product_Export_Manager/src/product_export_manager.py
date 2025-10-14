# product_export_manager.py
from pathlib import Path
import logging
from src.file_handler import read_csv, write_csv, write_json
from src.filter_tools import filter_ready_to_ship
from src.utils import setup_logging, log_action

# ------------------------------------------------------------
# 1️⃣ SETUP PATH
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "data" / "raw" / "products.csv"
PROCESSED_PATH = BASE_DIR / "data" / "processed"
LOG_PATH = BASE_DIR / "logs" / "app.log"

# Pastikan folder penting ada
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------
# 2️⃣ SETUP LOGGING
# ------------------------------------------------------------
setup_logging(LOG_PATH)

# ------------------------------------------------------------
# 3️⃣ FUNGSI UTAMA
# ------------------------------------------------------------
def main():
    try:
        # Baca data produk
        data = read_csv(RAW_PATH)
        if not data:
            print("⚠️ Tidak ada data yang ditemukan di file produk.")
            return
        
        # Filter data
        filtered_data = filter_ready_to_ship(data)
        
        # Simpan hasil filter ke CSV & JSON
        csv_out = PROCESSED_PATH / "ready_to_ship.csv"
        json_out = PROCESSED_PATH / "ready_to_ship.json"

        write_csv(csv_out, filtered_data, fieldnames=list(data[0].keys()))
        write_json(json_out, filtered_data)

        # Ringkasan hasil
        total = len(data)
        ready = len(filtered_data)

        print(f"\n📦 Total produk: {total}")
        print(f"🚚 Produk siap kirim: {ready}")
        print(f"✅ Data berhasil diekspor ke:\n - {csv_out}\n - {json_out}")

        # Logging
        log_action(LOG_PATH, f"Export sukses: {ready}/{total} produk ready to ship.")
        logging.info("Export selesai tanpa error.")

    except FileNotFoundError:
        print("❌ File 'products.csv' tidak ditemukan di folder data/raw.")
        logging.error("File produk tidak ditemukan.")
        log_action(LOG_PATH, "Gagal: file produk tidak ditemukan.")
    
    except Exception as e:
        print(f"⚠️ Terjadi kesalahan: {e}")
        logging.exception("Error tidak terduga.")
        log_action(LOG_PATH, f"Error tidak terduga: {e}")

# ------------------------------------------------------------
# 4️⃣ ENTRY POINT
# ------------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Menjalankan Product Export Manager...\n")
    main()
