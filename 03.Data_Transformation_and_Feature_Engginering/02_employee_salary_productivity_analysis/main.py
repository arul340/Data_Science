# ============================================================
# Mini Project 2: Employee Salary Productivity Analysis
# Author: Arul
# ============================================================

import json
from pathlib import Path
from src.data_loader import load_data
from src.utils import setup_logging
from src.data_cleaning import clean_data, detect_outliers_and_anomalies, normalize_data
from src.feature_engineering import create_features
from src.analysis import generate_report

def main ():
    # === Load Data ===

    # Load Config
    config_path = Path("config/settings.json")
    config = json.loads(config_path.read_text())

    # Load Logger
    logger = setup_logging(config["log_path"])
    logger.info("==== Pipeline Initialized ====")

    # Load Data
    df = load_data(config["raw_data_path"])
    logger.info(f"Data loaded: {df.shape}")

    # === Clean Data & Anomaly Detection ===

    # Handle Missing Value
    df = clean_data(df, config.get("missing_value_strategy", "mean"))
    logger.info("Cleaning complete")

    # Handle Outlier and Anomaly
    df = detect_outliers_and_anomalies(df, config.get("parameters", {}))
    logger.info("Outlier & anomaly handling complete")

    # Normalization
    df = normalize_data(df, config.get("parameters", {}))
    logger.info("Normalization complete")

    # === Feature engineering ====

    # Create Features
    df = create_features(df)
    logger.info("Feature engineering complete")

    # === Save processed ===
    processed_file_path = Path (config["processed_data_path"]) / "employee_cleaned.csv"
    processed_file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(processed_file_path, index=False)
    logger.info(f"Data saved to {processed_file_path}")

    # === Report ===
    report_file = Path(config["report_output_path"]) / "report.csv"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    generate_report(df, str(report_file))
    logger.info(f"Report saved to {report_file}")

    logger.info("==== Pipeline Completed ====")


if __name__ == "__main__":
    main()



    
