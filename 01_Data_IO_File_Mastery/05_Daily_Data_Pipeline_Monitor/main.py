from pathlib import Path
from src.utils import setup_logging, get_today_date, log_action
from src.file_handler import check_file_exists, read_csv, move_to_processed
from src.monitor import run_monitor_pipeline
from src.report_generator import generate_report


def main() :

    # 1. Specify the main directory
    BASE_DIR = Path(__file__).resolve().parent
    RAW_DIR = BASE_DIR / "data" / "raw"
    PROCESSED_DIR = BASE_DIR / "data" / "processd"
    REPORT_DIR = BASE_DIR / "data" / "reports"
    LOG_PATH = BASE_DIR / "logs" / "pipeline.log"

    # 2. Setup logging
    setup_logging(LOG_PATH)
    log_action("Memulai pipeline harian...")

    # 3. Run the monitoring process
    monitor_result = run_monitor_pipeline(RAW_DIR)

    # 4. Save the monitoring results report to CSV
    report_path = generate_report(REPORT_DIR, monitor_result)

    # If the file is valid, move it to the processed/ folder.
    if "details" in monitor_result: 
        for detail in monitor_result["details"] :
            if detail["status"] == "OK":
                file_path = RAW_DIR / detail["file_name"]
                move_to_processed(file_path, PROCESSED_DIR)

    log_action(f"Pipeline selesai. Laporan disimpan di {report_path}")
    log_action("Selesai...")


if __name__ == "__main__" :
    main()

