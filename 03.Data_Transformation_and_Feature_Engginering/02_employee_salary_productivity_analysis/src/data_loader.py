from pathlib import Path
import pandas as pd
from .log_info import log_info

def load_data(path: str) -> pd.DataFrame:
    # "Load a single CSV or concatenate multiple CSVs from a folder."
    raw_dir = Path(path)
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files :
        raise FileExistsError (f"No *.csv found in {raw_dir.resolve()}")
    
    if len(csv_files) == 1:
        df = pd.read_csv(csv_files[0])
        log_info(f"Data loaded from {csv_files[0].name}")
        return df
    
    df = pd.concat((pd.read_csv(file) for file in csv_files), ignore_index=True)
    log_info(f"Data loaded from {len(csv_files)} files")
    return df