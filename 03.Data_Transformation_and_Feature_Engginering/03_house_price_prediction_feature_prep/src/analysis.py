import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from .log_info import log_info
from pathlib import Path

def generate_report(df:pd.DataFrame, output_path:str, fig_path:str = None) -> None:
    # Summarize aggregates per salary category and save as CSV (single-level header).

    # ==== Rata-rata price_per_sqft per city ====
    summary = (
        df.groupby("city", dropna=False).agg({
            "price_per_sqft": "mean",
            "price": ["mean", "max", "min"],
            "area_sqft": "mean"
        }).round(2)
    )


    # Flatten multi-index columns: 
    summary.columns = ["_".join(col).strip() if isinstance(col, tuple) else col for col in summary.columns]
    report = summary.reset_index()

    # Save summary results to CSV
    report.to_csv(output_path, index=False)
    log_info(f"Report saved to {output_path}")

    # === Correlation between numerical features ===
    numeric_cols = df.select_dtypes(include='number')
    corr_matrix = numeric_cols.corr().round(2)

    # Also save to CSV file (optional)
    corr_csv_path = output_path.replace(".csv", "_correlation.csv")
    corr_matrix.to_csv(corr_csv_path)
    log_info(f"Correlation matrix saved to {corr_csv_path}")

    # === (Optional) Correlation heatmap ===
    if fig_path:
        Path(fig_path).parent.mkdir(parents=True, exist_ok=True)
        plt.figure(figsize=(8, 6))
        sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f")
        plt.title("Feature Correlation Heatmap")
        plt.tight_layout()
        plt.savefig(fig_path)
        plt.close()
        log_info(f"Correlation heatmap saved to {fig_path}")
