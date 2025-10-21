# Daily Data Pipeline Monitor.Py

# Start coding here...
import pandas as pd
from  pathlib import Path
from datetime import datetime
from src.utils import log_action

# Membuat laporan hasil monitoring pipeline harian dan menyimpannya ke file CSV.
def generate_report(report_dir: Path, monitor_result: dict) -> Path :

    # Make sure the report folder is available
    report_dir.mkdir(parents=True, exist_ok=True)

    # Get today's date
    today = datetime.now().strftime("%Y-%m-%d")

    # Specify the path for the report file.
    report_path = report_dir / f"pipeline_report_{today}.csv"

    # Get detailed results from monitor_result
    details = monitor_result.get("details", [])

    if not details:
        log_action("Tidak ada detail hasil monitoring untuk disimpan")
        df = pd.DataFrame([{
            "date": today,
            "status": monitor_result.get("status", "UNKNOWN"),
            "reason" : monitor_result.get("reason", "UNKNOWN"),
            "rows": 0
        }])

    else :
        df = pd.DataFrame(details)
        df["date"] = today

    # Save report to CSV file
    try:
        df.to_csv(report_path, index=False)
        log_action(f"Laporan monitoring berhasil disimpan: {report_path}")
        return report_path
    except Exception as error:
        log_action(f"Gagal menyimpan laporan monitoring: {error}")
        return None