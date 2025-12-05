from __future__ import annotations
from typing import Any, Dict
from .log_info import log_stage, log_info, log_error, log_success
from pathlib import Path

import pandas as pd

# -> HELPER FUNCTIONS 
def clean_curency(value):
    # Convert currency-like formats such as:
    # "Rp 12.500.000", "550.000", " 120000", "15.750.000", "Rp 15.200.000"
    # into float.

    if pd.isna(value):
        return None
    
    value = str(value)

    # Remove Rp. dots, commas, spaces
    value = (
        value.replace("Rp", "")
        .replace("rp", "")
        .replace(".", "")
        .replace(",", "")
        .strip()
    )

    if value == "":
        return None
    
    try:
        return float(value)
    except:
        return None
    
def clean_percent(value):
    # Convert percent-like formats:
    # "20%" → 0.20
    # "15" → 0.15
    # 0.95 (already OK)
    if pd.isna(value):
        return None
    
    value = str(value).strip()

    # Already decimal
    if value.replace(".", "", 1).isdigit():
        # Example: "0.95" or "15"
        v = float(value)
        return v if v <= 1 else v / 100
    
    # Example: "20%" -> 20 -> 0.20
    if value.endswith("%"):
        num = value.replace("%", "")
        try:
            return float(num) /100
        except:
            return None
        
    return None

def normalize_name(text):
    # Standardize product and category names.
    if pd.isna(text) or text is None:
        return None
    return str(text).strip().title()

# -> MAIN CLEANING FUNCTION
def clean_inventory_data(df: pd.DataFrame, logger=None) -> pd.DataFrame:
    # Perform full cleaning steps for retail invetory data.
    if logger:
        log_stage(logger, "Data Cleaning")

    df_clean = df.copy()

    # 1. Standirdize String Columns 
    str_cols = ["Product_ID", "Product_Name","Category", "Sub_Category"]
    for col in str_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(normalize_name)

    # 2. Clean Currency Columns
    currency_cols = ["Unit_Cost", "Selling_Price"]
    for col in currency_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(clean_curency)
    
    # 3. Clean Percent Columns
    percent_cols = ["Holding_Cost_Percent", "Service_Level_Target"]
    for col in percent_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(clean_percent)

    # 4. Convert Numeric Columns 
    qty_cols = [col for col in df_clean.columns if "Qty_Sold_" in col]
    numeric_cols = qty_cols + ["Opening_Stock_Jan", "Lead_Time_Days", "Ordering_Cost"]

    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")
    
    # 5. Handle Missing Product Names
    df_clean["Product_Name"] = df_clean["Product_Name"].fillna("Unknown Product")

    # 6. Handle Missing Ordering Cost
    if "Ordering_Cost" in  df_clean.columns:
        median_order_cost = df_clean["Ordering_Cost"].median()
        df_clean["Ordering_Cost"] = df_clean["Ordering_Cost"].fillna(median_order_cost)

    # 7. Remove Duplicates by Product ID
    if "Product_ID" in  df_clean.columns:
        before_dup = len(df_clean)
        df_clean = df_clean.sort_values(by=["Product_ID"]).drop_duplicates(subset=["Product_ID"], keep="first")
        after_dup = len(df_clean)
        if logger:
            log_info(logger, f"Removed {before_dup - after_dup} duplicated rows by Product ID.")

    # 8. Feature Engineering: Total Demand
    df_clean["Total_Demand"] = df_clean[qty_cols].sum(axis=1)

    if logger:
        log_success(logger, "Data Cleaning")
    
    return df_clean


# -> SAVE CLEANED DATA
def save_cleaned_data(df:pd.DataFrame, output_path: str, logger=None) -> bool:
    # Save processed data to processed folder.
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)

        if logger:
            log_info(logger, f"Cleaned data saved to {output_path}")
        return True
    except:
        if logger:
            log_error(logger, f"Failed to save cleaned data to {output_path}")
        return False