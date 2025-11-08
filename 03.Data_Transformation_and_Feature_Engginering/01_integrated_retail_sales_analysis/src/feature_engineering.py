import pandas as pd

def create_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "tanggal_transaksi" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["tanggal_transaksi"]):
        df["tanggal_transaksi"] = pd.to_datetime(df["tanggal_transaksi"], errors="coerce")

    # Total sales (using raw data before scaling)
    df["total_sales"] = df["jumlah"] * df["harga_satuan"] * (1 - df["diskon"])

    # Transaction month
    df["month"] = df["tanggal_transaksi"].dt.month

    # Popularity category
    df["popularity_category"] = df["jumlah"].apply(
        lambda x: "Best Seller" if x >= 15 else ("Medium Seller" if x >= 8 else "Low Seller")
    )

    # Margin & laba
    df["margin_profit"] = (df["harga_satuan"] * (1 - df["diskon"])) - (df["harga_satuan"] * 0.6)
    df["total_laba"] = df["margin_profit"] * df["jumlah"]

    return df
