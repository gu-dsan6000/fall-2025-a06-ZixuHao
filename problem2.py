#!/usr/bin/env python3
import argparse
import logging
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType


# ----------------------------- Logging ---------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
)
log = logging.getLogger("cluster_p2")


# ----------------------------- Constants --------------------------------
HOME = Path.home()
OUT_DIR = HOME / "spark-cluster"

TIMELINE_CSV = OUT_DIR / "problem2_timeline.csv"
CLUSTER_SUMMARY_CSV = OUT_DIR / "problem2_cluster_summary.csv"
STATS_TXT = OUT_DIR / "problem2_stats.txt"
BAR_PNG = OUT_DIR / "problem2_bar_chart.png"
DENSITY_PNG = OUT_DIR / "problem2_density_plot.png"

RE_APP_DIR = r"(application_(\d+)_(\d+))"
RE_TS_ISO = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
RE_TS_LEGACY = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"


@dataclass
class Paths:
    out_dir: Path = OUT_DIR
    timeline_csv: Path = TIMELINE_CSV
    summary_csv: Path = CLUSTER_SUMMARY_CSV
    stats_txt: Path = STATS_TXT
    bar_png: Path = BAR_PNG
    density_png: Path = DENSITY_PNG

    def ensure(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)


# ----------------------------- Spark ------------------------------------
def build_spark(master_url: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Problem2_Cluster_Usage")
        .master(master_url)
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider,"
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.requester.pays.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    log.info("Spark session started.")
    return spark


def save_single_csv(df, dest: Path):
    """将 Spark DataFrame 保存为单个 CSV 文件（含表头）。"""
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        empty = df.rdd.isEmpty()
    except Exception:
        empty = df.limit(1).count() == 0
    if empty:
        pd.DataFrame(columns=df.columns).to_csv(dest, index=False)
        return

    with tempfile.TemporaryDirectory(prefix="p2csv_") as tmpdir:
        tmp = Path(tmpdir)
        (df.coalesce(1)
           .write.mode("overwrite")
           .option("header", True)
           .csv(str(tmp)))
        part = next(tmp.glob("part-*.csv"), None)
        if not part:
            raise RuntimeError("No part-*.csv found.")
        shutil.move(str(part), str(dest))


# ----------------------------- Core Logic --------------------------------
def parse_logs(spark: SparkSession, s3_glob: str):
    df = spark.read.text(s3_glob).withColumn("path", F.input_file_name())
    if df.limit(1).count() == 0:
        raise SystemExit(
            f"No files matched: {s3_glob}\n"
            "Try:\n  hadoop fs -D fs.s3a.requester.pays.enabled=true "
            f"-ls '{s3_glob}' | head"
        )

    df = (
        df.withColumn("application_id", F.regexp_extract("path", RE_APP_DIR, 1))
          .withColumn("cluster_id",     F.regexp_extract("path", RE_APP_DIR, 2))
          .withColumn("app_number",     F.regexp_extract("path", RE_APP_DIR, 3).cast(IntegerType()))
    )

    ts_iso = F.regexp_extract("value", RE_TS_ISO, 1)
    ts_legacy = F.regexp_extract("value", RE_TS_LEGACY, 1)
    ts_col = F.coalesce(
        F.try_to_timestamp(ts_iso, "yyyy-MM-dd HH:mm:ss"),
        F.try_to_timestamp(ts_legacy, "yy/MM/dd HH:mm:ss"),
    )
    df = df.withColumn("ts", ts_col)

    apps = (
        df.where(F.col("application_id") != "")
          .where(F.col("ts").isNotNull())
          .groupBy("cluster_id", "application_id", "app_number")
          .agg(
              F.min("ts").alias("start_time"),
              F.max("ts").alias("end_time"),
          )
          .orderBy("cluster_id", "app_number")
    )

    cluster_summary = (
        apps.groupBy("cluster_id")
            .agg(
                F.count("*").alias("num_applications"),
                F.min("start_time").alias("cluster_first_app"),
                F.max("end_time").alias("cluster_last_app"),
            )
            .orderBy(F.col("num_applications").desc())
    )

    return apps.toPandas(), cluster_summary.toPandas()


# ----------------------------- Visualization --------------------------------
def write_stats_and_plots(paths: Paths, timeline: pd.DataFrame, summary: pd.DataFrame):
    paths.ensure()
    total_clusters = int(summary["cluster_id"].nunique()) if not summary.empty else 0
    total_apps = len(timeline)
    avg_apps = summary["num_applications"].mean() if not summary.empty else 0.0

    stats_lines = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps:.2f}",
        "",
        "Most heavily used clusters:",
    ]
    if not summary.empty:
        for _, row in summary.nlargest(5, "num_applications").iterrows():
            stats_lines.append(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} apps")
    paths.stats_txt.write_text("\n".join(stats_lines), encoding="utf-8")

    sns.set(style="whitegrid")

    # --- Bar Chart ---
    if not summary.empty:
        plt.figure(figsize=(10, 5))
        ordered = summary.sort_values("num_applications", ascending=False)
        ax = sns.barplot(
            data=ordered,
            x="cluster_id",
            y="num_applications",
            palette="Blues_d"
        )
        ax.set_title("Applications per Cluster")
        ax.set_xlabel("Cluster ID")
        ax.set_ylabel("Number of Applications")
        for p in ax.patches:
            h = p.get_height()
            ax.annotate(f"{int(h)}", (p.get_x() + p.get_width()/2, h),
                        ha="center", va="bottom", xytext=(0, 3),
                        textcoords="offset points", fontsize=9)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(paths.bar_png, dpi=150)
        plt.close()

    # --- Density Plot ---
    if not summary.empty:
        largest = str(summary.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"])
        tl = timeline.copy()
        tl["start_time"] = pd.to_datetime(tl["start_time"])
        tl["end_time"] = pd.to_datetime(tl["end_time"])
        tl["duration_sec"] = (tl["end_time"] - tl["start_time"]).dt.total_seconds()
        tl_big = tl[tl["cluster_id"] == largest]

        if not tl_big.empty:
            plt.figure(figsize=(8, 5))
            sns.histplot(tl_big["duration_sec"], bins=40, kde=True, color="darkcyan")
            plt.xscale("log")
            plt.xlabel("Job duration (seconds, log scale)")
            plt.ylabel("Count")
            plt.title(f"Duration Distribution — Cluster {largest} (n={len(tl_big)})")
            plt.tight_layout()
            plt.savefig(paths.density_png, dpi=150)
            plt.close()


# ----------------------------- CLI --------------------------------
def main():
    parser = argparse.ArgumentParser(description="Problem 2 — Cluster Usage Analysis (Refactored w/ Seaborn)")
    parser.add_argument("master_url", nargs="?", help="Spark master URL (e.g., spark://10.0.0.7:7077)")
    parser.add_argument("--net-id", help="Your NetID (for S3 path)")
    parser.add_argument("--input", default=None, help="Explicit log path (e.g., s3a://bucket/data/*/*.log)")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark; rebuild plots from CSVs.")
    args = parser.parse_args()

    paths = Paths()
    paths.ensure()

    if args.skip_spark:
        if not TIMELINE_CSV.exists() or not CLUSTER_SUMMARY_CSV.exists():
            raise SystemExit("Missing CSV files; cannot skip Spark.")
        timeline = pd.read_csv(TIMELINE_CSV)
        summary = pd.read_csv(CLUSTER_SUMMARY_CSV)
        write_stats_and_plots(paths, timeline, summary)
        print(f"Plots rebuilt:\n - {paths.bar_png}\n - {paths.density_png}\n - {paths.stats_txt}")
        return

    if not args.master_url or not (args.net_id or args.input):
        raise SystemExit("Provide master_url and either --net-id or --input for full run.")

    input_path = args.input or f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/container_*.log"
    spark = build_spark(args.master_url)
    timeline, summary = parse_logs(spark, input_path)

    timeline.to_csv(TIMELINE_CSV, index=False)
    summary.to_csv(CLUSTER_SUMMARY_CSV, index=False)

    save_single_csv(spark.createDataFrame(timeline), TIMELINE_CSV)
    save_single_csv(spark.createDataFrame(summary), CLUSTER_SUMMARY_CSV)

    write_stats_and_plots(paths, timeline, summary)

    print("Wrote outputs:")
    for f in [TIMELINE_CSV, CLUSTER_SUMMARY_CSV, STATS_TXT, BAR_PNG, DENSITY_PNG]:
        print("  ", f)


if __name__ == "__main__":
    main()
