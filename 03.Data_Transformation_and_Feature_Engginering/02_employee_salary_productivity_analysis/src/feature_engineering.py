import numpy as np
import pandas as pd

def create_features(df:pd.DataFrame) -> pd.DataFrame:
    # Create derived features and salary categorization (two versions: label & quantile).
    df = df.copy()

    # Salary per project (avoid zero division)
    df["salary_per_project="] = df["salary"] / (df["projects_done"] + 1e-6)

    # Efficiency score (avoid inf + chained assignment warning)
    eff = df["performance_score"] / (df["working_hours"] + 1e-6)
    eff = eff.replace([np.inf, -np.inf], np.nan)
    df["efficiency_score"] = eff.fillna(eff.median())

    # Manual threshold based salary categories (labels "Low/Medium/High")
    df["category_salary"] = pd.cut(
        df["salary"], bins = [0, 0.33, 0.66, 1.0],
        labels=["Low", "Medium", "High"],
        include_lowest=True
    )

    # Quantile version for alternative analysis (use name 'salary_range')
    df['salary_range'] = pd.qcut(df['salary'], q=3, labels=["Low", "Medium", "High"])

    return df