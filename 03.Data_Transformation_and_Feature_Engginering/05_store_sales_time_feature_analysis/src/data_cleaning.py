from __future__ import annotations

import pandas as pd
from typing import Any, Dict

from .utils import _get_numeric_cols, _get_categorical_cols 

def cleaning_data(df:pd.DataFrame, parameters:Dict[str, Any], logger) -> pd.DataFrame:
    try:
        df = df.copy()

        # === Required Cols ===
        required_cols = [
            "transaction_date",
            "store_id",
            "product_id",
            "customer_id",
            "quantity",
            "unit_price",
            "payment_method"
        ]

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            msg = f"Missing required columns: {missing_cols}"
            logger.error(msg)
            raise ValueError(msg)
        
        logger.info(f"All required columns are present: {required_cols}")

        # === Handle Missing Value ===
        strategy = parameters.get("missing_value_strategy", "mean").lower()
        numeric_cols = _get_numeric_cols(df)
        categorical_cols = _get_categorical_cols(df)

        if strategy == "mean":
            for col in numeric_cols:
                if df[col].isna().any():
                    mean_fill = df[col].mean()
                    df[col] = df[col].fillna(mean_fill)
                    logger.info(f"Missing values in '{col}' filled with mean: {mean_fill:.2f}")

        elif strategy == "median":
            for col in numeric_cols:
                if df[col].isna().any():
                    median_fill = df[col].median()
                    df[col] = df[col].fillna(median_fill)
                    logger.info(f"Missing values in '{col}' filled with median: {median_fill:.2f}")

        else:
            logger.info(f"Unknown missing_value_strategy='{strategy}', skipping numeric imputation.")

        for col in categorical_cols:
            if df[col].isna().any():
                mode_fill = df[col].mode(dropna=True)
                df[col] = df[col].fillna(mode_fill.iloc[0])
                logger.info(f"Missing values in '{col}' filled with mode: {mode_fill.iloc[0]}")

        # === Add Total Sales Columns ===
        df["total_sales"] = df["quantity"] * df["unit_price"]
        logger.info("Created total_sales column.")

        # === Validate Price and Quantity ===
        # 1. Payment Method Standartation standardization
        df["payment_method"] = df["payment_method"].str.lower().str.strip()

        # 2. Validaasi store_id & product_id pattern (ensure string type)
        df["store_id"] = df["store_id"].astype(str)
        df["product_id"] = df["product_id"].astype(str)

    
        # === Drop Duplicates ===
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)
        remove = before - after
        logger.info(f"Duplicated removed: {remove}")
            
        logger.info("Data cleaning completed successfully.")
        return df
    
    except Exception as error:
        logger.error(f"Error in cleaning data: {error}")
        return None
