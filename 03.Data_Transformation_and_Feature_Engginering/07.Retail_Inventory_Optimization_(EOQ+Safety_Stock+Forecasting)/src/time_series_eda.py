import os
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

def run_time_series_eda(df, output_folder="output/eda", decomposition=True):
    os.makedirs(output_folder, exist_ok=True)

    for sku_id, sku_df in df.groupby("Product_ID"):
        product_name = sku_df["Product_Name"].iloc[0]
        print(f"[INFO] Processing EDA for {sku_id} - {product_name}")

        sku_df = sku_df.sort_values("Date")
        sku_df["Date"] = pd.to_datetime(sku_df["Date"])
        sku_df = sku_df.set_index("Date")
        sku_df = sku_df.asfreq("MS")     # FIX frequency

        folder = os.path.join(output_folder, sku_id)
        os.makedirs(folder, exist_ok=True)

        y = sku_df["Qty_Sold"]

        # ======== Plot 1: Line Chart ========
        plt.figure(figsize=(10, 5))
        plt.plot(y.index, y.values)
        plt.title(f"{sku_id} - Time Series")
        plt.savefig(os.path.join(folder, "timeseries.png"))
        plt.close()

        # ======== Plot 2: ACF ========
        plt.figure(figsize=(10, 5))
        plot_acf(y, lags=6)
        plt.savefig(os.path.join(folder, "acf.png"))
        plt.close()

        # ======== Plot 3: PACF ========
        plt.figure(figsize=(10, 5))
        plot_pacf(y, lags=6, method="ywm")
        plt.savefig(os.path.join(folder, "pacf.png"))
        plt.close()

        # ======== Seasonal Decomposition ========
        if decomposition:
            try:
                result = seasonal_decompose(y, model="additive", period=12)
                result.plot()
                plt.savefig(os.path.join(folder, "decomposition.png"))
                plt.close()
            except Exception as e:
                print(f"[WARNING] Decomposition failed for {sku_id}: {e}")

        print(f"[OK] EDA saved → {folder}")

    print("\n=== ALL SKU EDA CLEANED & COMPLETED ===")


# import os
# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
# from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
# from statsmodels.tsa.seasonal import seasonal_decompose

# sns.set(style="whitegrid")

# def run_time_series_eda(df, output_folder="output/eda", decomposition=False):

#     # Ensure output folder exists
#     os.makedirs(output_folder, exist_ok=True)

#     # Pastikan kolom Date sudah datetime
#     df["Date"] = pd.to_datetime(df["Date"])

#     # Get unique SKUs
#     products = df["Product_ID"].unique()

#     for sku in products:
#         sku_folder = os.path.join(output_folder, sku)
#         os.makedirs(sku_folder, exist_ok=True)

#         # ambil data per SKU
#         sku_df = df[df["Product_ID"] == sku].copy().sort_values("Date")
#         product_name = sku_df["Product_Name"].iloc[0]

#         print(f"[INFO] Processing EDA for {sku} - {product_name}")

#         # 1) Line plot
#         plt.figure(figsize=(10, 5))
#         plt.plot(sku_df["Date"], sku_df["Qty_Sold"], marker="o")
#         plt.title(f"{sku} - {product_name}\nMonthly Sales")
#         plt.xlabel("Month")
#         plt.ylabel("Qty Sold")
#         plt.xticks(rotation=45)
#         plt.tight_layout()
#         plt.savefig(f"{sku_folder}/line_plot.png")
#         plt.close()

#         # 2) Rolling Mean 3 & 6 bulan
#         sku_df["SMA_3"] = sku_df["Qty_Sold"].rolling(3).mean()
#         sku_df["SMA_6"] = sku_df["Qty_Sold"].rolling(6).mean()

#         plt.figure(figsize=(10, 5))
#         plt.plot(sku_df["Date"], sku_df["Qty_Sold"], marker="o", label="Actual")
#         plt.plot(sku_df["Date"], sku_df["SMA_3"], label="SMA 3")
#         plt.plot(sku_df["Date"], sku_df["SMA_6"], label="SMA 6")
#         plt.title(f"{sku} - Rolling Mean Sales (3 & 6 months)")
#         plt.xlabel("Month")
#         plt.ylabel("Qty Sold")
#         plt.legend()
#         plt.xticks(rotation=45)
#         plt.tight_layout()
#         plt.savefig(f"{sku_folder}/rolling_mean.png")
#         plt.close()

#         # 3) ACF & PACF (lag dinamis)
#         max_lag = max(1, len(sku_df) // 2 - 1)

#         plt.figure(figsize=(10, 5))
#         plot_acf(sku_df["Qty_Sold"], lags=max_lag)
#         plt.title(f"{sku} - Autocorrelation (ACF)")
#         plt.tight_layout()
#         plt.savefig(f"{sku_folder}/acf.png")
#         plt.close()

#         plt.figure(figsize=(10, 5))
#         plot_pacf(sku_df["Qty_Sold"], lags=max_lag, method="ywm")
#         plt.title(f"{sku} - Partial Autocorrelation (PACF)")
#         plt.tight_layout()
#         plt.savefig(f"{sku_folder}/pacf.png")
#         plt.close()

#         # 4) Seasonal Decomposition (opsional)
#         if decomposition:
#             try:
#                 df_dec = sku_df.set_index("Date").asfreq("MS")

#                 decomposition_output = seasonal_decompose(
#                     df_dec["Qty_Sold"],
#                     model="additive",
#                     period=12
#                 )

#                 fig = decomposition_output.plot()
#                 fig.set_size_inches(12, 8)
#                 plt.suptitle(f"{sku} - Seasonal Decomposition")
#                 plt.tight_layout()
#                 plt.savefig(f"{sku_folder}/decomposition.png")
#                 plt.close()
#             except Exception as e:
#                 print(f"[WARNING] Decomposition failed for {sku}: {e}")

#         print(f"[OK] EDA saved → {sku_folder}")

#     print("\n=== ALL SKU EDA COMPLETED SUCCESSFULLY ===")

# if __name__ == "__main__":
#     df = pd.read_csv("output/features/inventory_timeseries.csv")
#     run_time_series_eda(df, output_folder="output/eda", decomposition=True)