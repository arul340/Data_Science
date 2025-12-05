from __future__ import annotations
from typing import Any, Dict, Optional, List
from pathlib import Path

import pandas as pd
import json

# -> Setup Configuration (settings.json)
def load_setting(path:str) -> Dict[str, Any] : 
    config_path = Path(path)

    if not config_path.exists():
        raise KeyError (f"[ERROR] setting.json not found at: {config_path.resolve()}")
    
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)
    
def ensure_directories(file_path: Path | str) -> None:
    file_path = Path(file_path)
    dir_path = file_path.parent

    if dir_path != Path("."):
        dir_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_path.parent.mkdir(parents=True, exist_ok=True)

def save_read_csv(path: Path | str, logger=None) -> Optional[pd.DataFrame]:
    path = Path(path)

    if not path.exists():
        if logger:
            logger.error(f"CSV file not found at: {path.resolve()}")
        return None
    
    try:
        df = pd.read_csv(path)
        if logger:
            logger.info(f"Loaded CSV: {path.name} shape: {df.shape}")
        return df
    except Exception as error:
        if logger:
            logger.error(f"Failed to read CSV at {path}: {error}")
        return None
    
def save_write_csv(df:pd.DataFrame, path: Path | str, logger=None) -> Optional[pd.DataFrame]:
    path = Path(path)

    try:
        df.to_csv(path, index=False)
        if logger:
            logger.info(f"Saved CSV: {Path(path).resolve()} shape: {df.shape}")
        return True
    except Exception as error:
        if logger:
            logger.error(f"Failed to save CSV at {path}:{error}")

def _get_numeric_cols(df:pd.DataFrame) -> list[str]:
    return [col for col in df.columns if df[col].dtype in ["int64", "float64", "int32", "float32"]]

def _get_categorical_cols(df:pd.DataFrame) -> List[str]:
    return [col for col in df.columns if df[col].dtype == "object"]

# -> Helper Min Max Scalling 
def min_max_scaller(series: pd.Series) -> tuple[pd.Series, float, float]:
    min_val = series.min()
    max_val = series.max()

    if min_val == max_val:
        scaled_series = pd.Series([0.0] * len(series), index=series.index)
    else:
        scaled_series = (series - min_val) / (max_val - min_val)

    return scaled_series, min_val, max_val