from pathlib import Path
import pandas as pd
from src.file_handler import check_file_exists, read_csv
from src.utils import log_action


# Memvalidasi isi DataFrame hasil pembacaan file CSV.
def validate_file(df: pd.DataFrame) -> dict:
    if df.empty:
        log_action("File CSV kosong, tidak ada data untuk diproses.")
        return {"status": "WARNING", "missing_columns" : [], "rows": 0}
    
    expected_cols = ["date", "product", "quantity", "price", "total", "region" ]
    missing_cols = [col for col in expected_cols if col not in df.columns]

    if missing_cols :
        log_action(f" Kolom berikut hilang dari data: {missing_cols}")
        return {"status": "FAILED", "missing_columns" : missing_cols, "rows": len(df) }
    
    log_action(f"File valid. Jumlah baris : {len(df)} Kolom: {list(df.columns)}")
    return {"status": "OK", "missing_columns" : [], "rows": len(df)}

# Check status daily pipeline
def check_pipeline_status(file_path: Path) -> dict:
    if not check_file_exists(file_path):
        return {
            "status" : "FAILED",
            "reason" : f"File not found: {file_path.name}",
            "rows" : 0
        }
    
    df = read_csv(file_path)
    result = validate_file(df)

    # Merge validation results into reports
    status = result["status"]
    if status == "OK":
        reason = "File valid dan siap diproses"
    elif status == "WARNING":
        reason = "File kosong, tidak ada data yang dapat diproses."
    else:
        reason = f"File tidak valid, kolom hilang: {result['missing_columns']}"

    return {
        "status" : status,
        "reason" : reason,
        "rows" : result["rows"]
    }

# Runs automatic check for all files in the raw folder.
def run_monitor_pipeline(raw_dir: Path) -> dict:
    log_action("Memulai monitoring pipeline harian...")

    if not raw_dir.exists():
        log_action("Folder raw tidak ditemukan.")
        return {"status": "FAILED", "reason": "Folder raw tidak ditemukan."}
    
    csv_files = list(raw_dir.glob("*.csv"))
    if not csv_files:
        log_action("Tidak ada file CSV di folder raw")
        return {"status": "WARNING", "reason": "Tidak ada file CSV ditemukan"}
    
    result = []
    for file_path in csv_files:
        log_action(f"Memeriksa file: {file_path.name}")
        status_info = check_pipeline_status(file_path)
        result.append({
            "file_name": file_path.name,
            "status" : status_info["status"],
            "reason" : status_info["reason"],
            "rows": status_info["rows"]
        })

    log_action("Monitoring selesai untuk semua file di folder raw")
    return {"status": "DONE", "details":result}


