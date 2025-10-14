# Sales Summary Program 
# Start Coding here ....

from pathlib import Path
from src.file_handler import read_sales, write_summary
from src.utils import setup_logging, to_int_safe, write_json, log_action

import logging

# === PATH SETUP ===
BASE_DIR  =  Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "raw" / "sales.csv"
SUMMARY_PATH = BASE_DIR / "data" / "processed" / "sales_summary.json"
JSON_PATH = BASE_DIR / "data" / "processed" / "sales_summary.json"
LOG_PATH = BASE_DIR / "logs" / "app.log"

# Pastikan folder penting sudah ada
DATA_PATH.parent.mkdir(parents = True, exist_ok = True)
SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# === MAIN FUNCTION ===
def main():
    # Fungsi utama untuk membaca data penjualan dan membuat ringkasan"
    try: 
        # Setup Logging
        setup_logging(LOG_PATH)
        logging.info("=== Program Sales Summary dimulai ===")

        # 1 Baca file CSV Menyah
        sales_data = read_sales(DATA_PATH)
        if not sales_data:
            logging.warning(f"File {DATA_PATH} kosong atau tidak berisi data.")
            return
        
        logging.info(f"Berhasil membaca file: {DATA_PATH}")

        # 2. AGREGASI TOTAL PER PRODUCT
        total_sales = {}

        for row in sales_data:
            product = row["product"]
            qty = to_int_safe(row["quantity"])
            price = to_int_safe(row["price"])
            subtotal = qty * price

            if product not in total_sales:
                total_sales[product]= {"total_quantity" : qty, "total_sales": subtotal} 
            else :
                total_sales[product]["total_quantity"] += qty
                total_sales[product]["total_sales"] += subtotal

        # 3. KONVERSI HASIL KE LIST OF DICT UNTUK OUTPUT
        summary_rows = [
            {"product": product,
             "total_quantity": data["total_quantity"],
             "total_sales": data["total_sales"]}
            for product, data in total_sales.items()
        ]

        # 4. Urutkan berdasarkan total_sales tertinggi
        summary_rows.sort(key=lambda x: x["total_sales"], reverse=True)

        # 5. Tulis hasil ke CSV
        fieldnames = ["product", "total_quantity", "total_sales"]
        write_summary(SUMMARY_PATH, summary_rows, fieldnames)
        logging.info(f"Hasil ringkasan disimpan di: {SUMMARY_PATH}")

        # 6. Simpan data ke JSON
        write_json(JSON_PATH, summary_rows)
        logging.info(f"Hasil ringkasan di simpan di {JSON_PATH}")

        log_action(LOG_PATH, "Sales summary berhasil dibuat tanpa error.")
        logging.info("=== Program Sales Summary selesai ===")

    except FileNotFoundError:
        log_action(LOG_PATH, f"File {DATA_PATH} tidak ditemukan")
        logging.error(f"file {DATA_PATH} tidak ditemukan.")

    except Exception as e:
        logging.exception("Terjadi error tak terduga")
        log_action(LOG_PATH, f"Terjadi error: {str(e)}")

# === JALANKAN MAIN FUNCTION ===
if __name__ == "__main__" :
    main()
                          

