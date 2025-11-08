import pandas as pd

def generate_report(df: pd.DataFrame, output_path: str) -> None:
    report = (
        df.groupby("kategori", dropna=False)
          .agg(total_sales=("total_sales", "sum"),
               avg_laba=("total_laba", "mean"),
               total_jumlah=("jumlah", "sum"))
          .reset_index()
    )
    report.to_csv(output_path, index=False)
