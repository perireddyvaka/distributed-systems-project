"""Generates clean publication-grade benchmark plots using Matplotlib.

Visualizes:
1. Search latency percentiles (p50, p95, p99) vs number of shards.
2. Search throughput (QPS) vs top-k ranking depths.
3. Incremental Recovery time vs Full Index Rebuild time.
4. Incremental Recovery speedup ratio and data transfer reduction percentage.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

# Set cache dir for Matplotlib to avoid permissions warning in sandbox
os.environ["MPLCONFIGDIR"] = "/tmp/matplotlib_cache"
import matplotlib.pyplot as plt
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("plot_results")


def plot_search_benchmarks(results_file: Path, output_dir: Path) -> None:
    """Plot search latency distributions and QPS throughput."""
    if not results_file.exists():
        logger.warning("Search results file not found: %s", results_file)
        return

    with open(results_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Plot Latency vs Number of Shards (at top_k=10)
    shards = []
    p50_vals = []
    p95_vals = []
    p99_vals = []
    qps_vals = []

    for shard_key in sorted(data.keys(), key=lambda x: int(x.split("_")[0])):
        shard_count = int(shard_key.split("_")[0])
        shards.append(f"{shard_count} Shard{'s' if shard_count > 1 else ''}")
        top10 = data[shard_key].get("top_10", {})
        p50_vals.append(top10.get("p50_latency_ms", 0))
        p95_vals.append(top10.get("p95_latency_ms", 0))
        p99_vals.append(top10.get("p99_latency_ms", 0))
        qps_vals.append(top10.get("qps", 0))

    x = np.arange(len(shards))
    width = 0.25

    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    fig, ax = plt.subplots(figsize=(8, 5))

    ax.bar(x - width, p50_vals, width, label="p50 (Median)", color="#2b5c8f")
    ax.bar(x, p95_vals, width, label="p95", color="#e67e22")
    ax.bar(x + width, p99_vals, width, label="p99", color="#c0392b")

    ax.set_ylabel("Search Latency (ms)", fontsize=12)
    ax.set_title("Distributed Search Latency Percentiles (Top-K = 10)", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(shards, fontsize=11)
    ax.legend(frameon=True)
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout()
    chart1_path = output_dir / "search_latency_vs_shards.png"
    plt.savefig(chart1_path, dpi=300)
    plt.close()
    logger.info("Saved %s", chart1_path)

    # 2. Plot QPS Throughput vs Top-K
    fig, ax = plt.subplots(figsize=(8, 5))
    top_k_labels = [5, 10, 20, 50]
    colors = ["#16a085", "#2980b9", "#8e44ad"]

    for idx, shard_key in enumerate(sorted(data.keys(), key=lambda x: int(x.split("_")[0]))):
        shard_count = int(shard_key.split("_")[0])
        qps_series = [data[shard_key].get(f"top_{k}", {}).get("qps", 0) for k in top_k_labels]
        ax.plot(
            top_k_labels,
            qps_series,
            marker="o",
            linewidth=2,
            label=f"{shard_count} Shard{'s' if shard_count > 1 else ''}",
            color=colors[idx % len(colors)],
        )

    ax.set_xlabel("Top-K Depth", fontsize=12)
    ax.set_ylabel("Throughput (Queries Per Second)", fontsize=12)
    ax.set_title("Search Throughput vs Top-K Depth", fontsize=13, fontweight="bold")
    ax.set_xticks(top_k_labels)
    ax.legend(frameon=True)
    ax.grid(True, linestyle="--", alpha=0.7)

    plt.tight_layout()
    chart2_path = output_dir / "search_throughput_vs_topk.png"
    plt.savefig(chart2_path, dpi=300)
    plt.close()
    logger.info("Saved %s", chart2_path)


def plot_recovery_benchmarks(results_file: Path, output_dir: Path) -> None:
    """Plot recovery time comparison, speedup ratio, and data reduction percentage."""
    if not results_file.exists():
        logger.warning("Recovery results file not found: %s", results_file)
        return

    with open(results_file, "r", encoding="utf-8") as f:
        data = json.load(f).get("benchmarks", [])

    if not data:
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    missed = [d["missed_updates"] for d in data]
    t_incr = [d["t_incremental_sec"] for d in data]
    t_full = [d["t_full_rebuild_sec"] for d in data]
    speedups = [d["speedup_ratio"] for d in data]
    data_reduction = [d["data_reduction_pct"] for d in data]

    # 1. Recovery Time: Incremental vs Full Rebuild
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(missed, t_incr, marker="o", color="#27ae60", linewidth=2.5, label="Incremental Recovery")
    ax.plot(missed, t_full, marker="s", color="#e74c3c", linewidth=2, linestyle="--", label="Full Index Rebuild")

    ax.set_xlabel("Number of Missed Updates", fontsize=12)
    ax.set_ylabel("Execution Time (seconds)", fontsize=12)
    ax.set_title("Recovery Duration: Incremental vs Full Rebuild", fontsize=13, fontweight="bold")
    ax.legend(frameon=True)
    ax.grid(True, linestyle="--", alpha=0.7)

    plt.tight_layout()
    chart3_path = output_dir / "recovery_time_incremental_vs_full.png"
    plt.savefig(chart3_path, dpi=300)
    plt.close()
    logger.info("Saved %s", chart3_path)

    # 2. Speedup & Data Reduction Subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Speedup
    ax1.plot(missed, speedups, marker="^", color="#2980b9", linewidth=2.5)
    ax1.set_xlabel("Number of Missed Updates", fontsize=11)
    ax1.set_ylabel("Speedup Ratio (T_full / T_incr)", fontsize=11)
    ax1.set_title("Incremental Recovery Speedup", fontsize=12, fontweight="bold")
    ax1.grid(True, linestyle="--", alpha=0.7)

    # Data Reduction
    ax2.plot(missed, data_reduction, marker="D", color="#8e44ad", linewidth=2.5)
    ax2.set_xlabel("Number of Missed Updates", fontsize=11)
    ax2.set_ylabel("Data Reduction (%)", fontsize=11)
    ax2.set_title("Network Payload Reduction (%)", fontsize=12, fontweight="bold")
    ax2.set_ylim(0, 105)
    ax2.grid(True, linestyle="--", alpha=0.7)

    plt.tight_layout()
    chart4_path = output_dir / "recovery_speedup_and_data_reduction.png"
    plt.savefig(chart4_path, dpi=300)
    plt.close()
    logger.info("Saved %s", chart4_path)


def main() -> None:
    search_file = Path("experiments/results/search_benchmarks.json")
    recovery_file = Path("experiments/results/recovery_benchmarks.json")
    plot_dir = Path("experiments/plots")

    plot_search_benchmarks(search_file, plot_dir)
    plot_recovery_benchmarks(recovery_file, plot_dir)


if __name__ == "__main__":
    main()
