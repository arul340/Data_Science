import pandas as pd
from pathlib import Path
from src.file_handler import read_excel
from src.data_cleaner import clean_dataframe

def calculate_total_value (df: pd.DataFrame) -> pd.DataFrame:
    # Tambahkan kolom total_value = Price * Quantity dan hitung total value per kombinasi (counttry, product)

    if df.empty:
        return df

    df["total_value"] = df["price"] * df["quantity"]

    # Hitung total value per negara dan product
    result = (
    df.groupby(["country", "product"], as_index=False)
      .agg({"total_value": "sum"})
    )


    return result



def get_top_products (df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    # Ambil to N product per negara berdasarkan total_value
    
    if df.empty:
        return df
    # urutkan dulu berdasarkan country dan total value
    df_sorted = df.sort_values(["country", "total_value"], ascending=[True, False])

    # Ambil to N product untuk tiap negara
    top_products =  df_sorted.groupby("country").head(top_n).reset_index(drop=True)

    return top_products

def merge_excel(raw_dir: Path) -> pd.DataFrame:
    # Gabungkan semua file Excel dari folder raw menjadi satu DataFrame bersih
    all_dfs = []

    for file in raw_dir.glob("*.xlsx"):
        print(f"Membaca file: {file.name}")
        df = read_excel(file)
        df_clean = clean_dataframe(df)
        all_dfs.append(df_clean)

    # setelah loop
    if not all_dfs:
        print("Tidak ada file Excel ditemukan.")
        return pd.DataFrame()

    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Total data tergabung: {len(combined_df)} baris")
    return combined_df


    
    
    
