from sklearn.cluster import KMeans
from pathlib import Path
from typing import Dict, Any
from .log_info import log_info, log_error, log_success
import pandas as pd
import matplotlib.pyplot as plt
import joblib

def run_kmeans_clustering(rfm_scaled_df: pd.DataFrame, model_output_path: str | Path, label_output_path: str | Path, summary_output: str | Path, elbow_output: str | Path, parameters: Dict [str, Any],  logger=None) -> pd.DataFrame:
    try:
        if logger:
            log_info(logger, "Starting KMeans Clustering...")

        df = rfm_scaled_df.copy()

        random_state = parameters.get("random_state", 42)
        n_cluster = parameters.get("n_cluster", 5)
        figure_size = parameters.get("figure_size", [8, 6])

        features = df[["recency_scaled", "frequency_scaled", "monetary_scaled"]]

        # === ELBOW METHOD ===
        innertia = []
        K = range (1, 8)

        for k in K:
            kmeans = KMeans(n_clusters=k, random_state=random_state)
            kmeans.fit(features)
            innertia.append(kmeans.inertia_)

        # Save elbow plot
        elbow_output = Path(elbow_output)

        if elbow_output.suffix == "":
            elbow_output.mkdir(parents=True, exist_ok=True)
            elbow_file = elbow_output / "elbow_plot.png"
        else:
            elbow_output.parent.mkdir(parents=True, exist_ok=True)
            elbow_file = elbow_output

        plt.figure(figsize=figure_size)
        plt.plot(K, innertia, marker='o')
        plt.title("Elbow Method")
        plt.xlabel("Number of clusters (k)")
        plt.ylabel("Innertia")
        plt.grid(True)
        plt.savefig(elbow_file)
        plt.close()

        if logger:
            log_success(logger, f"Elbow plot saved to {elbow_file.resolve()}")

        # === FIT FINAL MODEL (k=4)
        kmeans = KMeans(n_clusters=n_cluster, random_state=random_state)
        df["cluster"] = kmeans.fit_predict(features)

        model_output_path = Path(model_output_path)

        if model_output_path.suffix == "":
            model_output_path.mkdir(parents=True, exist_ok=True)
            model_file = model_output_path / "kmeans_model.pkl"
        else:
            model_output_path.parent.mkdir(parents=True, exist_ok=True)
            model_file = model_output_path

        joblib.dump(kmeans, model_file)

        if logger:
            log_info(logger, f"KMeans model saved to {model_file.resolve()}")

        # === CLUSTER LABELS RESULT ===
        df_labels = df.copy()

        label_output_path = Path(label_output_path)

        if label_output_path.suffix == "":
            label_output_path.mkdir(parents=True, exist_ok=True)
            label_file = label_output_path / "cluster_labels.csv"
        else:
            label_output_path.parent.mkdir(parents=True, exist_ok=True)
            label_file = label_output_path


        df_labels.to_csv(label_file, index=False)
       

        if logger :
            log_info(logger, f"Cluster labels saved to {label_file.resolve()}")

        # === SUMMARY BY CLUSTER ===
        summary = df.groupby("cluster")[["recency_scaled", "frequency_scaled", "monetary_scaled"]].mean().round(2)
        
        summary_output = Path(summary_output)
        if summary_output.suffix == "":
            summary_output.mkdir(parents=True, exist_ok=True)
            summary_file = summary_output / "cluster_summary.csv"
        else:
            summary_output.parent.mkdir(parents=True, exist_ok=True)
            summary_file = summary_output

        summary.to_csv(summary_file, index=False)

        if logger:
            log_info(logger, f"Cluster summary saved to {summary_file.resolve()}")

        return df_labels


    except Exception as error:
        if logger:
            log_error(logger, f"Error running KMeans Clustering: {error}")
        raise error
            