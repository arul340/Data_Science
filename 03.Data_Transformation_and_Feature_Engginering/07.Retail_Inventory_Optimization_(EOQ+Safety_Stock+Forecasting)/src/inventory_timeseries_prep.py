from __future__ import annotations

import pandas as pd

class TimeSeriesPrep:
    def __init__(self):
        self.month_map = {
            "Qty_Sold_Jan": "2025-01-01",
            "Qty_Sold_Feb": "2025-02-01",
            "Qty_Sold_Mar": "2025-03-01",
            "Qty_Sold_Apr": "2025-04-01",
            "Qty_Sold_May": "2025-05-01",
            "Qty_Sold_Jun": "2025-06-01",
            "Qty_Sold_Jul": "2025-07-01",
            "Qty_Sold_Aug": "2025-08-01",
            "Qty_Sold_Sep": "2025-09-01",
            "Qty_Sold_Oct": "2025-10-01",
            "Qty_Sold_Nov": "2025-11-01",
            "Qty_Sold_Dec": "2025-12-01"
        }

    def prepare(self, df: pd.DataFrame):

        qty_columns = list(self.month_map.keys())

        # Melt wide  -> log
        df_long = df.melt(
            id_vars=["Product_ID", "Product_Name", "Category", "Sub_Category"],
            value_vars=qty_columns,
            var_name="Month",
            value_name="Qty_Sold",
        )

        # Convert Month -> Date
        df_long["Date"] = df_long["Month"].map(self.month_map)
        df_long["Date"] = pd.to_datetime(df_long["Date"], errors="coerce")

        # Sorting
        df_long = df_long.sort_values(["Product_ID", "Date"]).reset_index(drop=True)

        # Clean unnecessary column
        df_long = df_long.drop(columns=["Month"])

        # save to csv
        df_long.to_csv("output/features/inventory_timeseries.csv", index=False)

        return df_long