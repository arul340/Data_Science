import pandas as pd
from pathlib import Path

from .log_info import log_info, log_error, log_success, log_stage

def load_data(raw_path: str, logger) -> pd.DataFrame:
    log_stage(logger, "Data Loading")

    try:
        raw_path: Path = Path(raw_path)

         # === CASE 1: If path points to a single CSV file ===
        if raw_path.is_file() and raw_path.suffix == ".csv":
            df = pd.read_csv(raw_path)
            log_info(logger, f"Loaded single file: {raw_path.name} ({df.shape[0]} rows) ({df.shape[1]} cols) ")

        # === CASE 2:  Directory with multiple CSV ===
        elif raw_path.is_dir():
            csv_files = sorted(list(raw_path.glob("*.csv")))
            if not csv_files:
                raise ValueError(f"No CSV files found in folder: {raw_path.resolve()}")
        
            dfs =[]
            for file in csv_files:
                df_part = pd.read_csv(file)
                log_info(logger, f"Loaded file: {file.name} ({df_part.shape[0]} rows) ({df_part.shape[1]} cols)")
                dfs.append(df_part)

            df = pd.concat(dfs, ignore_index=True)
            log_info(logger, f"Data loaded from {len(csv_files)} files: Total ({df.shape[0]} rows) ({df.shape[1]} cols) ")

        else:
            raise FileNotFoundError (f"Invalid path: {raw_path.resolve()} (not a CSV file or directory)")
        
        required_cols = [
            "transaction_date",
            "store_id",
            "product_id",
            "customer_id",
            "quantity",
            "unit_price",
            "payment_method"
        ]

        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        else:
            log_success(logger, f"Data loaded successfully from {raw_path.resolve()}")
            log_info(logger, f"All required columns found: {required_cols}")

        # Sanity Check
        dup_count = df.duplicated(subset=["customer_id"]).sum()
        if dup_count > 0:
            log_info(logger, f"Found {dup_count} duplicate customer IDs (will be handled later).")
        else:
            log_info(logger, "No duplicate customer IDs found.")

        log_success(logger, "Data Loading")
        return df
    
    except Exception as error:
        log_error(logger, f"Failed to load data: {error}")
        return None