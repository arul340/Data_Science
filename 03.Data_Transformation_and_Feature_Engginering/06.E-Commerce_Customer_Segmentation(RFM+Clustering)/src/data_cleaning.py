from __future__ import annotations
from typing import Any, Dict
from .log_info import log_info, log_error, log_success
from .utils import _get_categorical_cols, _get_numeric_cols

import pandas as pd

def clean_data(df: pd.DataFrame, logger ) -> pd.DataFrame:
    df = df.copy()
    log_info(logger, "Starting Data Cleaning...")
    try:

        # -> Handle Duplicates
        dup_count  = df.duplicated().sum()
        if dup_count > 0:
            log_info(logger, f"Found {dup_count} duplicate rows. Dropping duplicates...")
        df = df.drop_duplicates()

        # -> Check Required Cols
        required_cols = [
            "order_id",
            "customer_id",
            "order_date",
            "amount",
            "status",
            "product_category",
            "region",
            "payment_method",
        ]

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols :
            message = f"Missing required columns: {missing_cols}"
            if logger:
                logger.error(message)
            raise ValueError(message)
        
        #  -> Parse Date (Support Multiple Formats)
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

        missing_dates = df["order_date"].isna().sum()
        if missing_dates > 0 :
            log_info(logger, f"Dropping {missing_dates} rows with invalid dates...")
        df = df.dropna(subset=["order_date"]) # date is critical

        #  -> Validate & Clean Costumer_id
        before = len(df)
        df = df[df["customer_id"].astype(str).str.match(r"^C\d+$")]
        dropped_invalid = before - len(df)
        if dropped_invalid > 0:
            log_info(logger, f"Dropped {dropped_invalid} rows with invalid customer")

        # -> Clean Numeric Column "amount"
        df["amount"] = pd.to_numeric(df["amount"],errors="coerce")
        missing_amount = df["amount"].isna().sum()
        if missing_amount > 0:
            log_info(logger, f"Dropping {missing_amount} rows with invalid amount...")
            df = df.dropna(subset=["amount"])
        
        category_cols = ["status", "product_category", "region", "payment_method"]

        for col in category_cols:
            df[col] = (
                df[col].astype(str).str.strip().str.lower()
            )
        
        #  -> Fix specific formatting
        df["product_category"] = df["product_category"].str.replace("&", "and", regex=False).str.lower()
        df["status"] = df["status"].str.lower()
        df["region"] = df["region"].str.lower()

        #  -> Outlier Detection for 'amount' (IQR Method)
        Q1 = df["amount"].quantile(0.25)
        Q3 = df["amount"].quantile(0.75)
        IQR = Q3 - Q1 
        upper = Q3 + 1.5 * IQR

        df["is_outlier_amount"] = df["amount"] > upper
        outlier_count = df["is_outlier_amount"].sum()

        if outlier_count > 0:
            log_info(logger, f"Detected {outlier_count} outlier transaction (flagged, not removed).")

        log_success(logger, "Data Cleaning Completed Successfully")
        return df
    
    except Exception as error:
        log_error (logger, f"Failed to clean data. Error: {error}")
        return None






