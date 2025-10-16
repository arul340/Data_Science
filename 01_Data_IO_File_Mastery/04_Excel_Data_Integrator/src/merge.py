import pandas as pd
from pathlib import Path
from src.data_cleaner import clean_dataframe
from src.file_handler import read_excel

def merge_excels (raw_dir: Path) -> pd.DataFrame:
    # Menggabungkan semua file Excel dalam folder raw

    all_dfs = []

    for file in raw_dir.glob("*.xlsx"):
        print(f"Membaca file: {file.name}")
        df = read_excel(file)
        df_clean = clean_dataframe(df)
        all_dfs.append(df_clean)
    
    if not all_dfs:
        print("Tidak ada file Excel ditemukan .")
        return pd.DataFrame()
    
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Total data tergabung: {len(combined_df)} baris")
    return combined_df