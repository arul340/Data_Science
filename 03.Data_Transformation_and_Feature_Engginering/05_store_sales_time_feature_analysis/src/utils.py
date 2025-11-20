from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, List
import json

import pandas as pd
import numpy as np

# === Load Configuration (setting.json) ===
def load_settings(path:str) -> Dict[str, Any]:
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError (f"[ERROR] settings.json not found at: {config_path.resolve()}")
    
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)


# === Ensure Directory Exist ===
def ensure_directories(file_path: Path | str) -> None:
    file_path = Path(file_path)
    dir_path = file_path.parent

    if dir_path != Path('.'):
        dir_path.parent.mkdir(parents=True, exist_ok=True)


# === Save CSV Reader with Logging ===
def save_read_csv(path: Path | str, logger=None) -> Optional[pd.DataFrame]:
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
            logger.error(f"Failed to read CSV at {path} : {error}")
        return None
        

# === Safe CSV writter with logging ===
def save_write_csv(df: pd.DataFrame, path: Path | str, logger=None) -> Optional[pd.DataFrame]:
    path = Path(path)
    ensure_directories(path)

    try:
        df.to_csv(path, index=False)
        if logger:
            logger.info(f"Saved CSV: {Path(path).resolve()} shape: {df.shape}")
        return True
    
    except Exception as error:
        if logger:
            logger.error(f"Failed to save CSV at {path}: {error}")
        return False
    
# === Helper: Detect numeric + categorical columns ===
# get Numeric Columns
def _get_numeric_cols(df:pd.DataFrame) -> List[str]:
    return df.select_dtypes(include=[np.number]).columns.tolist()

# get Categorical Columns
def _get_categorical_cols(df:pd.DataFrame) -> List[str]:
    return df.select_dtypes(include=["object"]).columns.to_list()


# ===  Helper: Min-Max Scaling ===
def _min_max_scaling(series: pd.Series) ->tuple[pd.Series,float, float]:
    min_val = series.min()
    max_val = series.max()

    if min_val == max_val:
        scaled_series = pd.Series([0.0] * len(series), index=series.index)
    else:
        scaled_series = (series - min_val) / (max_val - min_val)
    return scaled_series, min_val, max_val