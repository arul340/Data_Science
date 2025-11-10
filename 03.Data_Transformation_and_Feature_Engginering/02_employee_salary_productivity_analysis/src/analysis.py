import pandas as pd
from .log_info import log_info

def generate_report (df:pd.DataFrame, output_path:str) -> None:
    # Summarize aggregates per salary category and save as CSV (single-level header).
    report = df.groupby("category_salary", dropna=False, observed=True).agg({
        "efficiency_score": "mean",
        "working_hours" : "mean",
        "salary": ["mean", "max", "mean"]
    })

    # # Flatten multi-index columns: ('salary','mean') -> 'salary_mean'
    report.columns = ["_".join(col).strip() if isinstance(col, tuple) else col for col in report.columns]
    report = report.reset_index()

    report.to_csv(output_path, index=False)
    log_info(f"Report saved to {output_path}")