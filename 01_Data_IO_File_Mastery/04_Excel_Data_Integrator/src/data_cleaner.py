# Start coding here...
import pandas as pd

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Membersihkan dan menormalisasikanFrame
    if df.empty:
        return df
    
    # Ubah semua nama kolom ke lowercase
    df.columns = [col.strip().lower() for col in df.columns]

    # Hapus duplikat
    df  = df.drop_duplicates()

    # Isi nilai kosong (kalau ada)
    df = df.fillna( {
        "category" : "Unknown",
        "country": "Unknown",
        "price" : 0,
        "quantity": 0
    })


    # pastikan tipe data numerik benar
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    return df
