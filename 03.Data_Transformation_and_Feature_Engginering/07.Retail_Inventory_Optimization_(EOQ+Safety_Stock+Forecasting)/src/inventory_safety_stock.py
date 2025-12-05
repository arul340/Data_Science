from __future__ import annotations
from .log_info import log_info, log_stage, log_success

import pandas as pd
import numpy as np

# -> Z-SCORE MAPPING FUNCTION
def map_z_score(service_level: float) -> float:
    # Convert service level target to corresponding Z-score.
    z_table = {
        0.90: 1.28,
        0.92: 1.41,
        0.95: 1.65,
        0.98: 2.05,
        0.99: 2.33
    }

    # nearest mapping approach
    closest = min(z_table.keys(), key=lambda x: abs(x - service_level))
    return z_table[closest]

# -> MAIN SAFETY STOCK MODULE
def calculate_safety_stock_and_rop(df: pd.DataFrame, output_path, logger=None) -> pd.DataFrame:
    if logger:
        log_stage(logger, "Safety Stock & Reorder Point Calculation")

    df_calc = df.copy()

    # 1. Map Z-score
    df_calc["Z_Score"] = df_calc["Service_Level_Target"].apply(map_z_score)

    # 2. Safety Stock 
    df_calc["Safety_Stock"] = (
        df_calc["Z_Score"] * df_calc["Std_Dev_Monthly_Demand"] * np.sqrt(df_calc["Lead_Time_Days"])
    )

    # 3. Reorder Point (ROP)
    df_calc["Reorder_Point"] = (
        df_calc["Avg_Daily_Demand"] *df_calc["Lead_Time_Days"] + df_calc["Safety_Stock"]
    )

    # 4. Save to CSV
    df_calc.to_csv(output_path, index=False)

    if logger:
        log_success(logger, "Safety Stock & Reorder Point Calculation Completed")

    return df_calc
