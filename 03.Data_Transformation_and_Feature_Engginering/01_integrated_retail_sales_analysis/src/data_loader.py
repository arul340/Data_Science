from pathlib import Path
import pandas as pd

def load_data(path: str, merge_key: str) -> pd.DataFrame:
    
    # Load sales_*.csv then merge with store_info.csv.
    p = Path(path)
    sales_files = sorted(p.glob("sales_*.csv"))
    if not sales_files:
        raise FileNotFoundError(f"No sales_*.csv found in {p.resolve()}")

    df_sales = pd.concat([pd.read_csv(f) for f in sales_files], ignore_index=True)
    df_store = pd.read_csv(p / "store_info.csv")
    return df_sales.merge(df_store, on=merge_key, how="left")
