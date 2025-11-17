from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

# === Load Configuration (setting.json) ===
def load_settings(path: str) -> Dict[str, Any]:
    # Load JSON settings file.
    #  - Args: Path to settings.json
    #  - Returns: Dictionary representation of the configuration

    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"[ERROR] setting.json not found at: {config_path.resolve()}")
    
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)
    
# Ensure directory exists before saving files.
def ensure_directories(file_path: Path | str) -> None:
    # Create parent directory of a file if it doesn't exist
    # Example: ensure_directory("output/reports/report.csv")

    file_path = Path(file_path)
    dir_path = file_path.parent

    if not dir_path.exists():
        dir_path.mkdir(parents=True, exist_ok=True)

# Save CSV reader with logging
def safe_read_csv(path: Path | str, logger=None) -> Optional[pd.DataFrame]:
    # Read CSV safely with optional logger.
    # Returns: DataFrame or None if failed.

    path = Path(path)

    if not path.exists():
        if logger:
            logger.error(f"CSV file not found at: {path.resolve()}")
        return None
    
    try:
        df = pd.read_csv(path)
        if logger:
            logger.info(f"Loaded CSV: {path.name}, shape: {df.shape}")
        return df
    
    except Exception as error:
        if logger:
            logger.error(f"Failed to read CSV at {path}: {error}")
        return None
    
# Save CSV writer with logging
def safe_write_csv(df: pd.DataFrame, path: Path | str, logger=None) -> bool:
    # Write DataFrame to CSV safely.
    # Returns: True if success, False otherwise.

    path = Path(path)
    ensure_directories(path)

    try:
        df.to_csv(path, index=False)
        if logger: 
            logger.info(f"Saved CSV: {path.resolve()} (shape={df.shape})")
        return True
    
    except Exception as error:
        if logger:
            logger.error(f"Failed to save CSV at {path}: {error}")
        return False
    
# Helper: Detect numeric + categorical columns
def get_numeric_columns(df:pd.DataFrame):
    return df.select_dtypes(include=["number"]).columns.tolist()

def get_categorical_columns(df:pd.DataFrame):
    return df.select_dtypes(include=["object"]).columns.tolist()

# Helper: Min-Max Scaling
def min_max_scale(series: pd.Series) -> pd.Series:
    # Min-Max Normalize a pandas Series safely.
    # If series constant -> return 0.

    min_val = series.min()
    max_val = series.max()

    if min_val == max_val:
        return pd.Series([0.0] * len(series), index=series.index)
    
    return (series - min_val) / (max_val - min_val)