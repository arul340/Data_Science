import csv
import json
from pathlib import Path

# File Handler.Py module
# =================================================
# Membaca File CSV
# =================================================
def read_csv(file_path: Path) :
    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return list(reader)
    
# =================================================
# Menulis Ulang File CSV
# =================================================
def write_csv(file_path: Path, data: list[dict], fieldnames: list[str]) :
    try: 
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"Berhasil menulis file: {file_path}")
    except Exception as error:
        print(f"Gagal menulis CSV: {error}")

# =================================================
# Menulis Ulang File CSV
# =================================================
def write_json(file_path: Path, data) :
    try: 
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

        print(f"Berhasil menulis file: {file_path}")
    except Exception as error:
        print(f"Gagal menulis JSON: {error}")

        