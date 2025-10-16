import pandas as pd


def clean_dataframe(df:pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    
    # Ubah semua nama kolom ke lowercase
    df.columns = [col.strip().lower() for col in df.columns]

    # Hapus duplikat
    df = df.drop_duplicates()

    # Isi nilai kosong(fillna) 
    df = df.fillna({
        "category": "Unknown",
        "country": "Unknown",
        "price": 0,
        "quantity" : 0
    })

    # Konvesi kolom numerik
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    return df
