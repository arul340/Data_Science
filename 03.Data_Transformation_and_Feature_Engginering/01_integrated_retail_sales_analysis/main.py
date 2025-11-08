# ============================================================
# Mini Project 1: Integrated Retail Sales Analysis
# Author: Arul
# ============================================================
import json
from pathlib import Path
from src.data_loader import load_data
from src.utils import setup_logging
from src.data_cleaning import clean_data, detect_outliers_and_anomalies, convert_and_normalize
from src.feature_engineering import create_features
from src.analysis import generate_report

def main():
    # 1) Load config
    config_path = Path("config/settings.json")
    config = json.loads(config_path.read_text())

    # 2) Logger
    logger = setup_logging(config["log_path"])
    logger.info("==== Pipeline Initialized ====")

    # 3) Load
    df = load_data(config["raw_data_path"], config["merge_key"])
    logger.info(f"Data loaded: {df.shape}")

    # 4) Clean
    df = clean_data(df, config['missing_value_strategy'])
    logger.info("Cleaning complete")

    # 5) Outlier / anomaly
    df = detect_outliers_and_anomalies(df, config["parameters"])
    logger.info("Outlier & anomaly handling complete")

    # 6) Feature engineering (sebelum normalisasi)
    df = create_features(df)
    logger.info("Feature engineering complete")

    # 7) Normalisasi kolom numerik terpilih
    df = convert_and_normalize(df, config["parameters"])
    logger.info("Normalization complete")

    # 8) Save processed
    Path(config["processed_data_path"]).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(config["processed_data_path"], index=False)
    logger.info(f"Processed saved to {config['processed_data_path']}")

    # 9) Report
    Path(config["report_output_path"]).parent.mkdir(parents=True, exist_ok=True)
    generate_report(df, config["report_output_path"])
    logger.info(f"Report saved to {config['report_output_path']}")

    logger.info("==== Pipeline Completed ====")

if __name__ == "__main__":
    main()
