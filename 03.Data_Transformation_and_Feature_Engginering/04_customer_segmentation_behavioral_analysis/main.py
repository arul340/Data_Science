from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from src.log_info import setup_logger, log_stage, log_error, log_success
from src.utils import load_settings, ensure_directories

from src.data_cleaning import clean_customer_data
from src.feature_engineering import engineer_customer_features
from src.analysis import generate_customer_report

import pandas as pd

def run_pipeline(config_path: str = "config/settings.json") -> None:
    # === Load configuration ===
    config: Dict[str, Any] = load_settings(config_path)
    paths = config.get("paths", {})

    input_paths = paths.get("input", {})
    output_paths = paths.get("output", {})
    logging_cfg = paths.get("logging", {})

    raw_data_path = Path(input_paths.get("raw_data", "data/raw/raw_customer_behavior.csv"))
    cleaned_data_path = Path(output_paths.get("cleaned_data", "data/processed/customer_behavior_cleaned.csv"))
    features_path = Path(output_paths.get("features", "output/features/customer_features.csv"))
    report_path = Path(output_paths.get("reports", "output/reports/customer_segment.csv"))
    log_file = Path(logging_cfg.get("log_file", "logs/pipeline.log.txt"))

    parameters: Dict[str, Any] = config.get("parameters", {})

    # === Setup Logger ===
    logger = setup_logger()
    log_stage(logger, "Customer Segmentation Pipeline Started")

    # === Load Raw Data ===
    log_stage(logger, "Data Loading")
    if not raw_data_path.exists():
        msg = f"Raw data not found at: {raw_data_path.resolve()}"
        log_error(logger, msg)
        raise FileNotFoundError(msg)
    
    df_raw = pd.read_csv(raw_data_path)
    df_clean = clean_customer_data(df_raw, parameters, logger)
    
    # Make sure directory for data cleaned exists
    ensure_directories(cleaned_data_path)
    df_clean.to_csv(cleaned_data_path, index=False)
    log_success(logger, f"Cleaned data saved to: {cleaned_data_path}")

    # === Feature Engineering ===
    log_stage(logger, "Feature Engineering")
    df_features = engineer_customer_features(df_clean, parameters, logger)

    ensure_directories(features_path)
    df_features.to_csv(features_path, index=False)
    log_success(logger, f"Feature data saved to: {features_path}")

    # === Analysis & Reporting ===
    log_stage(logger, "Analysis & Reporting")
    ensure_directories(report_path)
    generate_customer_report(df_features, report_path, logger)
    log_success(logger, f"Report saved to: {report_path}")

    # === Done ===
    log_stage(logger, "Pipeline Completed")

if __name__ == "__main__":
    run_pipeline()