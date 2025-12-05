from __future__ import annotations
from .log_info import log_info, log_stage, log_success, log_error

import pandas as pd
import numpy as np

def calculate_eoq(df: pd.DataFrame, output_path:str, logger=None) -> pd.DataFrame:
    # Calculate EOQ using:
    # EOQ = sqrt( (2 * D * S) / H )
    # Where:
    #     D = Annual Demand
    #     S = Ordering Cost
    #     H = Holding Cost (per unit per year)

    if logger:
        log_stage(logger, "EOQ Calculation")

    df_eoq = df.copy()

    # EOQ
    df_eoq["EOQ"] = np.sqrt(
        (2* df_eoq["Annual_Demand"] * df_eoq["Ordering_Cost"]) / df_eoq["Holding_Cost"]
    )

    # Save to CSV
    df_eoq.to_csv(output_path, index=False)

    if logger:
        log_success(logger, "EOQ Calculation Completed")

    return df_eoq