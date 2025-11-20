import pandas as pd
from pathlib import Path
from typing import Dict, Any

from src.log_info import setup_logger, log_stage, log_info, log_success, log_error
from src.utils import load_settings, save_write_csv
from src.loader_data import load_data
from src.data_cleaning import cleaning_data
from src.features_engineering import time_features, sales_features, customer_behavior
from src.analysis import generate_report

def main_pipeline(config_path: str = "config/settings.json"):
    # --- Load Config & Setup Logger---
    try:
        settings = load_settings(config_path)
    except Exception as error:
        print( f"FATAL ERROR: Failed to load configuration from {config_path}. "
            f"Check file existence and JSON format. Error: {error}")
        return
    
    logger = setup_logger(config_path)
    log_stage(logger, "Pipeline Initialization")
    log_info(logger, f"Configuration loaded successfully from {config_path}")

    # Extract Path and Parameter
    paths: Dict[str, Any] = settings.get("paths", {})
    params: Dict[str, Any] = settings.get("parameters", {})

    # Use the key that is actually in settings.json
    raw_data_path: str = paths.get("input", {}).get(
        "raw_data", "data/raw/store_sales_raw.csv"
    )

    processed_data_path: str = paths.get("output", {}).get(
        "cleaned_data", "data/processed/store_sales_cleaned.csv"
    )

    report_output_dir: str = paths.get("output", {}).get(
        "reports", "output/reports"
    )

    # Create output directory if not available
    Path(report_output_dir).parent.mkdir(parents=True, exist_ok=True)
    Path(processed_data_path).parent.mkdir(parents=True, exist_ok=True)


    # --- 2. Data Loading ---
    log_stage(logger, "Data Loading")
    df_raw = load_data(raw_data_path, logger)
    if df_raw is None:
        log_error(logger, f"Pipeline stooped due to Data Loading failure.")
        return
    
    # --- 3. Data Cleaning ---
    log_stage(logger, "Data Cleaning")
    cleaning_params = params  #Get parameter cleaning
    df_cleaned = cleaning_data(df_raw, cleaning_params, logger)
    if df_cleaned is None:
        log_error(logger, "Pipeline stopper due to Data Cleaning failure.")
        return
    
    #  --- 4 Feature Engineering ---
    log_stage(logger, "Feature Engineering")
    try:
        # Conversion to datetime is a prerequisite for time_features
        if "transaction_date" in df_cleaned.columns:
            df_cleaned["transaction_date"] = pd.to_datetime(df_cleaned["transaction_date"], errors="coerce")
            df_cleaned.dropna(subset=["transaction_date"], inplace=True)
            log_info(logger, "Converted 'transaction_date' to datetime format and drop invalid rows.")
        else:
            log_error(logger, "Column 'transaction_date' not found. Feature engineering cannot processed")
            return
        
        # Time Feature Engineering
        df_features = time_features(df_cleaned)
        log_info(logger, f"Time features engineered. Shape: {df_features.shape}")

        # Sales Feature Engineering
        df_features = sales_features(df_features)
        log_info(logger, f"Sales features engineered. Shape: {df_features.shape}")

        # Custumer Behavior Feature Engineering
        df_final = customer_behavior(df_features)
        log_info(logger, f"Customer behavior features engineered. Shape: {df_final.shape}")

        log_success(logger, f"Feature Engineering")
    
    except Exception as error:
        log_error(logger, f"Failed during Feature Engineering: {error}")

    # Save the cleaned and feature engineered data
    save_write_csv(df_final, processed_data_path, logger)


    #  --- 5. Generate Report / Analyisis ---
    log_stage(logger, "Analysis and Reporting")
    analysis_results = generate_report(df_final, report_output_dir, logger)

    if analysis_results is None:
        log_error(logger, "Pipeline stopped due to Analysis and Reporting failure.")
    
    log_success(logger, "Pipeline Finished")

if __name__ == "__main__":
    main_pipeline(config_path="config/settings.json")