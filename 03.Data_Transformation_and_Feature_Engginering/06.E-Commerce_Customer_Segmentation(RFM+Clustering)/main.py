from __future__ import annotations
from pathlib import Path
from typing import Any, Dict
from src.log_info import setup_logger, log_stage, log_info, log_warning, log_error, log_success
from src.utils import load_settings, ensure_directories, save_write_csv
from src.data_loader import load_data
from src.data_cleaning import clean_data
from src.rfm_features import compute_rfm_features
from src.rfm_scalling import scale_rfm_features
from src.kmeans_model import run_kmeans_clustering


import pandas as pd

def main_pipeline (config_path: str = "config/settings.json"):
    # === Load Configuration ===
    try:
        setting_config = load_settings(config_path)
    except Exception as error:
        print(
            f"FATAL ERROR: Failed to load configuration from {config_path}." 
            f"Check file existence and JSON format. Error: {error}"
            )
        
    logger = setup_logger(config_path)
    log_stage(logger, "Pipeline Initialization")
    log_info(logger, f"Configuration loaded successfully from {config_path}")

    # -> Extract Path and Parameters
    Paths : Dict[str, Any] = setting_config.get("path", {})
    parameters : Dict[str, Any] = setting_config.get("parameters", {})

    # Use the key that is actually in settings.json
    raw_data_path : str = Paths.get("input", {}).get(
        "raw_data", "data/raw/transactions.csv"
    )

    processed_data_path : str = Paths.get("output", {}).get(
        "cleaned_data", "data/processed/transactions_cleaned.csv"
    )

    features_engineering_path : str = Paths.get("output", {}).get(
        "features_engineering", "output/features_enginering/"
    ) 

    model_output_path = "output/model/kmeans_model.pkl"
    label_output_path = "output/model/cluster_labels.csv"
    summary_output_path = "output/model/cluster_summary.csv"
    elbow_output_path = "output/model/elbow_plot.png"

    # Create directory if not exist
    ensure_directories(processed_data_path)

    # === Load Raw Data ===
    log_stage(logger, "Data Loading")
    df_raw = load_data(raw_data_path, logger)
    if df_raw is None:
        message = f"Pipeline stooped due to Data Loading failure."
        log_error(logger, message)
        return
    
    # === Clean Raw Data ===
    log_stage(logger, "Data Cleaning")
    # parameters = parameters
    df_clean = clean_data(df_raw, logger)

    # -> Save Cleaned Data
    log_stage(logger, "Save Cleaned Data")
    save_write_csv(df_clean, processed_data_path, logger)

    log_success(logger, f"Data cleaning success and saved to {processed_data_path}")

    # === Compute RFM Features ===
    log_stage(logger, "Compute RFM Features")
    df_rfm = compute_rfm_features(df_clean, features_engineering_path, logger)

    # == Scale RFM Scaled ==
    log_stage(logger, "Scalling RFM Features")
    df_rfm_scaled = scale_rfm_features(df_rfm, features_engineering_path, parameters, logger)

    # === KMeans Clustering ===
    log_stage(logger, "KMeans Clustering")
    df_cluster = run_kmeans_clustering(
        df_rfm_scaled, model_output_path, label_output_path, summary_output_path, elbow_output_path, parameters, logger
    )
    
if __name__ == "__main__":
    main_pipeline(config_path="config/settings.json")






