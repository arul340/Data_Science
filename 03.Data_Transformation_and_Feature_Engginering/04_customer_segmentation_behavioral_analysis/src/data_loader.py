import pandas as pd
from pathlib import Path
from src.log_info import log_stage, log_info, log_success, log_error

def load_data(raw_path: str, logger) -> pd.DataFrame:
    # Load raw customer behavioral data and validate required columns.

    log_stage(logger, "Data Loading")

    try:
        raw_dir = Path(raw_path)

        # === CASE 1: If path points to a single CSV file ===
        if raw_dir.is_file() and raw_dir.suffix == ".csv":
            df = pd.read_csv(raw_dir)
            log_info(logger, f"Loaded single file: {raw_dir.name} ({df.shape[0]} rows), ({df.shape[1]} cols)")

        # === CASE 2: If path points to a directory ===
        elif raw_dir.is_dir():
            csv_files = sorted(list(raw_dir.glob("*.csv")))
            if not csv_files:
                raise ValueError(f"No CSV files found in folder: {raw_dir.resolve()}")
            
            dfs = []
            for file in csv_files:
                df_part = pd.read_csv(file)
                log_info(logger, f"Loaded file: {file.name} ({df_part.shape[0]} rows)" )
                dfs.append(df_part)
            
            df = pd.concat(dfs, ignore_index=True)
            log_info(logger, f"Data loaded from {len(csv_files)} file(s): Total {df.shape[0]} rows, {df.shape[1]} cols")
        
        else:
            raise FileNotFoundError(f"Invalid path: {raw_dir.resolve()} (not a CSV file or directory)")

        # Validate required columns
        required_cols = ["customer_id",
            "age",
            "gender",
            "annual_income_usd",
            "spending_score",
            "visits_per_month",
            "avg_transaction_value",
            "days_since_last_purchase",
            "city",] 
        missing = [col for col in required_cols if col not in df.columns]

        if missing:
            log_error(logger, f"Missing required columns: {missing}")
            raise ValueError(f"Missing required columns: {missing}")
        else:
            log_info(logger, f"All required columns found: {required_cols}")

        # === Sanity Checks ===
        dup_count = df.duplicated(subset=["customer_id"]).sum()
        if dup_count > 0:
            log_info(logger, f"Found {dup_count} duplicate customer IDs (will be handled later).")
        else:
            log_info(logger, "No duplicate customer IDs found.")

        log_success(logger, "Data Loading")
        return df
    except Exception as error:
        log_error(logger, f"Error loading data: {error}")
        raise