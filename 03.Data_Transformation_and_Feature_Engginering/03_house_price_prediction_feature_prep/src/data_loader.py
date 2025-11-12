from pathlib import Path
import pandas as pd
from .log_info import log_info

def load_data (path: str) -> pd.DataFrame:
    #  "Load a single CSV or concatenate multiple CSVs from a folder."
    raw_dir = Path(path)
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No *.csv found in {raw_dir.resolve()}")
    
    if len(csv_files) == 1:
        df = pd.read_csv(csv_files[0])
        log_info(f"Data loaded from {csv_files[0].name}")      
    else:
        df = pd.concat((pd.read_csv(file) for file in csv_files), ignore_index=True)
        log_info(f"Data loaded from {len(csv_files)} files")
    
    # Validation Required Columns
    required_cols = ["price", "area_sqft", "bedrooms"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    else:
        log_info(f"All required columns found: {required_cols}")
        
    return df

    



    
