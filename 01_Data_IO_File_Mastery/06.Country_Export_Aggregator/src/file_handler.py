from pathlib import Path
import pandas as pd

def read_excel(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        return pd.DataFrame()
    
    try:
        return pd.read_excel(file_path)
    except Exception as error:
        print(f"Error reading Excel file: {error}")
        return pd.DataFrame()
    
def write_excel (file_path: Path, df: pd.DataFrame, sheet_name="data"):
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"File Excel berhasil disimpan di {file_path}")

    except Exception as error:
        print(f"Gagal menulis file excel: {error}")

def write_csv (file_path: Path, df: pd.DataFrame):
    try:
        df.to_csv(file_path, index=False)
        print (f"File CSV berhasil disimpan di {file_path}")
    
    except Exception as error:
        print(f"Gagal menulis file CSV: {error}")