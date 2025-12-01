from __future__ import annotations
from pathlib import Path

from .log_info import log_info, log_warning, log_error, log_success, log_stage

import pandas as pd

def load_data(raw_path: str, logger) -> pd.DataFrame:
    log_stage(logger, "Data Loading")

    try:
        raw_dir = Path(raw_path)

        # Case 1 -> If paths points to a single CSV files.
        if raw_dir.is_file() and raw_dir.suffix == ".csv":
            df = pd.read_csv(raw_dir)
            log_info(logger, f"Loaded single file: {raw_dir.name} ({df.shape[0]} rows) ({df.shape[1]} cols)")

        # Case 2 -> Directory with multiple CSV
        elif  raw_dir.is_dir():
            csv_files = sorted(list(raw_dir.glob("*.csv")))
            if not csv_files:
                raise ValueError(f"No CSV files found in folder: {raw_dir.resolve()}")
            
            dfs = []
            for file in csv_files:
                df_part = pd.read_csv(file)
                log_info(logger, f"Loaded file: {file.name} ({df_part.shape[0]} rows) ({df_part.shape[1]} cols)")
                dfs.append(df_part)

            df = pd.concat(dfs, ignore_index=True)
            log_info(logger, f"Data loaded from {len(csv_files)} file. Total ({df.shape[0]} rows) ({df.shape[1]} cols)")
        else :
            raise FileNotFoundError(f"Invalid path: {raw_dir.resolve()} (Not a CSV file or directory)")
        
        # Validate Columns
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
            message = f"Missing required columns: {missing_cols}"
            log_warning(logger, message)
        else:
            log_info(logger, "All required columns are present.")    

        log_success(logger, "Data Loading Completed")
        return df
    
    except Exception as error:
        log_error(logger, f"Failed to load data: {error}")
        return None


