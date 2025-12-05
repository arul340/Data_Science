from __future__ import annotations
from typing import Any, Dict
from src.log_info import setup_logger, log_stage, log_info, log_error, log_success
from src.utils import load_setting, ensure_directories
from src.data_loader import load_raw_data
from src.data_cleaning import clean_inventory_data, save_cleaned_data
from src.inventory_features import calculate_feature_engineering
from src.inventory_eoq import calculate_eoq
from src.inventory_safety_stock import calculate_safety_stock_and_rop
from src.inventory_timeseries_prep import TimeSeriesPrep

# ⬇️ Tambah import baru
from src.time_series_eda import run_time_series_eda
from src.forecasting import run_forecasting


def main_pipeline(config_path: str = "config/settings.json"):

    # -> Load Configuration
    try:
        setting_config = load_setting(config_path)
    except Exception as error:
        print(
            f"FATAL ERROR : Failed to load configuration from {config_path}."
            f"Check file existence and JSON format. Error: {error}"
        )

    logger = setup_logger(setting_config)
    log_stage(logger, "Pipeline Initialization")
    log_info(logger, f"Configuration loaded successfully from {config_path}")

    # Extract Paths
    Paths: Dict[str, Any] = setting_config.get("paths", {})

    raw_data_path = (
        Paths.get("input", {})
            .get("raw_data", "data/raw/raw_inventory_data.csv")
    )

    data_cleaned_path = (
        Paths.get("output", {}).get("cleaned_data", "data/processed/inventory_data_cleaned.csv")
    )

    inventory_features_path = (
        Paths.get("output", {}).get("inventory_features", "output/features/inventory_features.csv")
    )

    inventory_eoq_path = (
        Paths.get("output", {}).get("inventory_eoq", "output/features/inventory_eoq.csv")
    )

    safety_stock_and_rop_path = (
        Paths.get("output", {}).get("safety_stock_and_rop", "output/features/safety_stock_and_rop.csv")
    )

    # Load Raw Data
    log_stage(logger, "Data Loading")
    df_raw = load_raw_data(raw_data_path, logger)
    if df_raw is None:
        log_error(logger, "Pipeline stopped due to Data Loading Failure.")
        return

    # Data Cleaning
    log_stage(logger, "Data Cleaning")
    df_cleaned = clean_inventory_data(df_raw, logger)
    save_cleaned_data(df_cleaned, data_cleaned_path, logger)

    # Feature Engineering
    log_stage(logger, "Inventory Optimization")
    df_feat = calculate_feature_engineering(df_cleaned, inventory_features_path, logger)

    # EOQ
    log_stage(logger, "EOQ Calculation")
    df_eoq = calculate_eoq(df_feat, inventory_eoq_path, logger)

    # Safety Stock + ROP
    log_stage(logger, "Safety Stock and Reorder Point Calculation")
    df_calc = calculate_safety_stock_and_rop(df_eoq, safety_stock_and_rop_path, logger)

    # Time-Series Prep
    log_stage(logger, "Time Series Prep")
    ts_prep = TimeSeriesPrep()
    df_timeseries = ts_prep.prepare(df_calc)

    # ==========================================================
    #  🔍 TIME-SERIES EDA  (Optional → Controlled via config)
    # ==========================================================
    ENABLE_EDA = setting_config.get("eda", {}).get("enable_eda", False)

    if ENABLE_EDA:
        log_stage(logger, "Time-Series EDA")
        run_time_series_eda(
            df_timeseries=df_timeseries,
            output_folder="output/eda/"
        )

    # ==========================================================
    #  📈 FORECASTING
    # ==========================================================
    FORECAST_MONTHS = setting_config.get("forecasting", {}).get("forecast_horizon", 3)

    log_stage(logger, "Forecasting")
    run_forecasting(
        df_timeseries=df_timeseries,    output_folder="output/forecasts/",
        forecast_horizon=FORECAST_MONTHS
    )


    # DONE
    log_stage(logger, "Pipeline Complete")
    print("\n=== ALL TASKS FINISHED SUCCESSFULLY ===")


if __name__ == "__main__":
    main_pipeline(config_path="config/settings.json")
