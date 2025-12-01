import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# === 1. Load Clustered Data ===

def load_clustered_data(path: str) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Cluster label file not found: {path}")
    df = pd.read_csv(path)
    return df

# === 2. Heatmap RFM Profile per Cluster ===
def plot_heatmap(df:pd.DataFrame, output_path: Path) -> pd.DataFrame:
    cluster_profile = (
       df.groupby("cluster")[["recency_scaled", "frequency_scaled", "monetary_scaled"]].mean()
    )
    # Heatmap
    plt.figure(figsize=(8, 5))
    sns.heatmap(cluster_profile, annot=True, cmap="Blues", fmt=".2f")
    plt.title("RFM Profile per Cluster")
    plt.ylabel("Cluster")
    plt.xlabel("RFM Metric")
    plt.tight_layout()
    output_path = output_path / "cluster_heatmap.png"
    plt.savefig(output_path)
    plt.close()
    print (f"[OK] Heatmap saved - {output_path}")

    return cluster_profile

# === 3. Scatter Plot (Frequency vs Monetary) per Cluster ===
def plot_scatter_frequency_monetary(df:pd.DataFrame, output_path: Path) -> None:
    plt.figure(figsize=(8, 6))
    sns.scatterplot(
        data=df,
        x="frequency_scaled",
        y="monetary_scaled",
        hue="cluster",
        palette="Set2",
        s=80,
        alpha=0.8
    )

    plt.title("Scatter: Frequency vs Monetary by Cluster")
    plt.xlabel("Frequency (scaled)")
    plt.ylabel("Monetary (scaled)")
    plt.grid(True, alpha=0.2)
    plt.tight_layout()

    output_path = output_path / "scatter_frequency_monetary.png"
    plt.savefig(output_path)
    plt.close()
    print(f"[OK] Scatter Frequency-Monetary saved - {output_path}")

# === 4. Scatter Plot (Recency vs Monetary) ===
def plot_scatter_recency_monetary(df:pd.DataFrame, output_path: Path) -> None:
    plt.figure(figsize=(8, 6))
    sns.scatterplot(
        data=df,
        x="recency_scaled",
        y="monetary_scaled",
        hue="cluster",
        palette="Set2",
        s=80,
        alpha=0.8
    )
    plt.title("Scatter: Recency vs Monetary by Cluster")
    plt.xlabel("Recency (scaled)")
    plt.ylabel("Monetary (scaled)")
    plt.grid(True, alpha=0.3)

    output_path = output_path / "scatter_recency_monetary.png"
    plt.savefig(output_path)
    plt.close()
    print(f"[OK]  Scatter Recency - Monertary saved -> {output_path}")

#  === 5. Barplot (Average RFM Metrics per Cluster) ===
def plot_bar_rfm (df:pd.DataFrame, output_path: Path) -> None:
    cluster_profile = (df.groupby("cluster") [["recency_scaled", "frequency_scaled", "monetary_scaled"]].mean().reset_index()
    )

    melted = cluster_profile.melt(
        id_vars="cluster",
        var_name="metric",
        value_name="value"
    )

    plt.figure(figsize=(9,6))
    sns.barplot(
        data=melted,
        x="metric",
        y="value",
        hue="cluster",
        palette="Set2"
    )

    plt.title("Average RFM Metrics per Cluster")
    plt.xlabel("RFM Metric")
    plt.ylabel("Mean (scaled)")
    plt.tight_layout()

    output_path = output_path / "barplot_rfm_metrics.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(str(output_path), bbox_inches="tight")
    plt.close()
    print(f"[OK] Barplot RFM saved -> {output_path}")

# === Save Cluster Summary Table + Simple Text Profilling
def save_summary_and_profilling(df:pd.DataFrame, output_path: Path) -> None:
    profile = df.groupby("cluster")[["recency_scaled", "frequency_scaled", "monetary_scaled"]].mean()

    # Save CSV Summary
    summary_path = output_path / "cluster_summary.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    profile.to_csv(summary_path)
    print(f"[OK] Cluster summary CSV saved -> {summary_path}")

    # Simple text profilling
    lines = []
    lines.append("CLUSTER PROFILING (AUTO-GENERATED)")
    lines.append("==================================\n")

    for cluster in profile.index:
        recency = profile.loc[cluster, "recency_scaled"]
        frequency = profile.loc[cluster, "frequency_scaled"]
        monetary = profile.loc[cluster, "monetary_scaled"]

        lines.append(f"Cluster {cluster}:")
        lines.append(f"Recency (Mean Scaled): {recency:.2f}")
        lines.append(f"Frequency (Mean Scaled): {frequency:.2f}")
        lines.append(f"Monetary (Mean Scaled): {monetary:.2f}")

        # Simple Interpretation
        if recency > 0.6 :
            lines.append(f" →  Pelanggan cenderung tidak aktif / hampir churn.")
        elif recency < 0.3:
            lines.append(f" →  Pelanggan cukup aktif (recent buyers).")

        if frequency > 0.6:
            lines.append(" →  Pelanggan sering bertransaksi (Loyal/High-Value).")
        elif frequency < 0.3:
            lines.append(" →  Pelanggan tidak sering bertransaksi (Low-Value).")

        if monetary > 0.6:
            lines.append(" →  High spender / nilai traksaksi tinggi")
        elif monetary < 0.3:
            lines.append(" →  Low spender / nilai transaksi rendah.")
        
        lines.append("")

    txt_path = output_path / "cluster_profilling.txt"
    txt_path.parent.mkdir(parents=True, exist_ok=True)
    with open(txt_path, "w", encoding="utf-8") as file :
        file.write("\n".join(lines))

    print(f"[OK] Cluster profilling TXT saved -> {txt_path}")

# === Runner - Run All Functions ===
def run_cluster_analysis(cluster_csv_path: str = "output/model/cluster_labels.csv", output_path: str = "output/analysis") -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print ("=== Running Cluster Analysis (Standalone) ===")

    df = load_clustered_data(cluster_csv_path)

    plot_heatmap(df, output_path)
    plot_scatter_recency_monetary(df, output_path)
    plot_scatter_frequency_monetary(df, output_path)
    plot_bar_rfm(df, output_path)
    save_summary_and_profilling(df, output_path)

    print ("=== Cluster Analysis Completed! ===")


if __name__ == "__main__":
    run_cluster_analysis()
