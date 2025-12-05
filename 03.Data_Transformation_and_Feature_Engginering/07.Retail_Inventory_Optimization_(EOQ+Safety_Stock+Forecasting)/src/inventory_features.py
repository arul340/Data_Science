from __future__ import annotations

from .log_info import log_info, log_stage, log_success, log_error

import pandas as pd
import numpy as np

def calculate_feature_engineering(df:pd.DataFrame, output_path: str, logger=None) -> pd.DataFrame:
    # Create all required inventory optimization features:
    # - Annual Demand
    # - Average Daily Demand
    # - Standard Deviation of Monthly Demand
    # - Holding Cost (per unit per year)

    if logger:
        log_stage(logger, "Feature Engineering")

    df_feat = df.copy()
    
    # 1. Monthly Demand Columns
    qty_cols = [col for col in df.columns if "Qty_Sold" in col]

    # 2. Annual Demand
    df_feat["Annual_Demand"] = df_feat["Total_Demand"]

    # 3. Average Daily Demand
    df_feat["Avg_Daily_Demand"] = df_feat["Annual_Demand"] / 365

    # 4. Standard Deviation (Monthly)
    df_feat["Std_Dev_Monthly_Demand"] = df_feat[qty_cols].std(axis=1)

    # 5. Holding Cost (per Unit per Year)
    # Holding_Cost_Percent sudah berupa decil, contoh 0.2 = 20%
    df_feat["Holding_Cost"] = df_feat["Holding_Cost_Percent"] * df_feat["Unit_Cost"]

    df_feat.to_csv(output_path, index=False)

    if logger:
        log_success(logger, "Feature Engineering Completed")

    return df_feat


