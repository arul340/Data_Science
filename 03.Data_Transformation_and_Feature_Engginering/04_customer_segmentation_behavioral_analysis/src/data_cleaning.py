from __future__ import annotations
from typing import Any, Dict, List
import numpy as np
import pandas as pd

def _get_numeric_columns(df:pd.DataFrame) -> List[str]:
    # Return the list of numeric columns in the DataFrame.
    return df.select_dtypes(include=[np.number]).columns.tolist()

def _get_categorical_columns(df:pd.DataFrame) -> List[str]:
    # Return the list of categorical (object) columns in dataframe.
    return df.select_dtypes(include=["object"]).columns.tolist()

def clean_customer_data(df:pd.DataFrame, parameters:Dict[str, any], logger) -> pd.DataFrame:
    # Clean raw customer behavior data:
    # - Validate required columns
    # - Drop duplicates
    # - Handle missing values (numeric & categorical)
    # - Normalize selected numeric features to [0,1] range

    df = df.copy()

    # === Required columns check ===
    required_cols = [
        "customer_id",
        "age",
        "gender",
        "annual_income_usd",
        "spending_score",
        "visits_per_month",
        "avg_transaction_value",
        "days_since_last_purchase",
        "city",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        msg = f"Missing required columns: {missing_cols}"
        logger.error(msg)
        raise ValueError(msg)
    
    logger.info(f"All required columns are present: {required_cols}")

    # === Drop duplicates ===
    before = len(df)
    df = df.drop_duplicates(subset="customer_id", keep="first")
    after = len(df)
    dropped = before - after
    if dropped > 0:
        logger.warning(f"Dropped {dropped} duplicates rows based on customer_id.")
    else:
        logger.info("No duplicates found.")

    # === Handle missing values ===
    strategy = parameters.get("missing_value_strategy", "mean").lower()
    numeric_cols = _get_numeric_columns(df)
    category_cols = _get_categorical_columns(df)

    # Numeric : Fill with mean (default)
    if strategy == "mean":
        for col in numeric_cols:
            if df[col].isna().any():
                mean_val = df[col].mean()
                df[col] = df[col].fillna(mean_val)
                logger.info(f"Filled NaN in numeric column '{col}' with mean={mean_val:.2f}")

    elif strategy == "median":
        for col in numeric_cols:
            if df[col].isna().any():
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
                logger.info(f"Filled NaN in numeric column '{col}' with median={median_val:.2f}")

    else:
        logger.warning(f"Missing values strategy '{strategy}' not supported.")

    # Categorical: fill with mode
    for col in category_cols:
        if df[col].isna().any():
            mode = df[col].mode(dropna=True)
            if len(mode) > 0:
                fill_val = mode.iloc[0]
                df[col] = df[col].fillna(fill_val)
                logger.info(f"Filled NaN in categorical column '{col}' with mode = {fill_val}")
            else:
                logger.warning(f"Column '{col}' has NaN but no mode; leaving as is.")

    # === Normalize selected numeric features to (Min-Max Scaling) ===
    # Hanya fitur yang relevan untuk model/analisis yang dinormalisasi
    cols_to_scale = [
        "age",
        "annual_income_usd",
        "spending_score",
        "visits_per_month",
        "avg_transaction_value",
        "days_since_last_purchase",
    ]

    for col in cols_to_scale:
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found, skipping normalization.")
            continue

        col_min = df[col].min()
        col_max = df[col].max()
        if col_max == col_min:
            logger.warning(
                f"Column '{col}' has constant value (min=max={col_min}). "
                "Skipping normalization and setting scaled values to 0."
            )
            df[col] = 0.0
        else:
            df[col] = (df[col] - col_min) / (col_max - col_min)
            logger.info(
                f"Normalized column '{col}' using Min-Max scaling "
                f"(min={col_min:.4f}, max={col_max:.4f})."
            )

    logger.info(f"Cleaning complete. Final shape: {df.shape}.")
    return df