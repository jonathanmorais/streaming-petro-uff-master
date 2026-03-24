#!/usr/bin/env python3
"""
Plota métricas do benchmark Spark Structured Streaming — dataset 3W.

Uso:
  python benchmark_simple/plot.py
  python benchmark_simple/plot.py --results benchmark_simple/results
  python benchmark_simple/plot.py --results benchmark_simple/results --output benchmark_simple/plots
"""
import argparse
import json
from pathlib import Path

import numpy as np

DEFAULT_RESULTS_DIR = Path(__file__).parent / "results"
DEFAULT_OUTPUT_DIR  = Path(__file__).parent / "plots"

COLOR = "#E25A1C"  # laranja Spark


def load_metrics(results_dir: Path) -> dict:
    for name in ("spark_metrics.json", "metrics.json"):
        f = results_dir / name
        if f.exists():
            with open(f) as fh:
                return json.load(fh)
    raise FileNotFoundError(f"Nenhum arquivo de métricas encontrado em {results_dir}")


def plot_all(metrics: dict, output_dir: Path) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        try:
            import seaborn as sns
            sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
        except ImportError:
            pass
    except ImportError:
        print("[ERRO] matplotlib não encontrado. Instale: pip install matplotlib")
        _print_metrics_table(metrics)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Figura 1: Painel de métricas principais (throughput + latência)
    # ------------------------------------------------------------------
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle(
        "Spark Structured Streaming — Dataset 3W (Petrobras, EKS)",
        fontsize=13, fontweight="bold",
    )

    # 1.1 Throughput
    ax = axes[0]
    thr = metrics.get("throughput_rps", 0)
    bar = ax.bar(["Spark"], [thr], color=COLOR, width=0.4, edgecolor="white")
    ax.text(bar[0].get_x() + bar[0].get_width() / 2,
            thr + thr * 0.02, f"{thr:,.0f}", ha="center", va="bottom", fontsize=11)
    ax.set_title("Throughput")
    ax.set_ylabel("Registros / segundo")
    ax.set_ylim(0, thr * 1.3)
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)

    # 1.2 Latência por percentil
    ax = axes[1]
    percentiles = ["P50", "P95", "P99", "Média"]
    keys = ["latency_p50", "latency_p95", "latency_p99", "latency_mean"]
    vals = [metrics.get(k, 0) / 1000 for k in keys]  # ms → s
    bars = ax.bar(percentiles, vals, color=COLOR, edgecolor="white", alpha=0.9)
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v + max(vals) * 0.01,
                f"{v:.1f}s", ha="center", va="bottom", fontsize=9)
    ax.set_title("Latência por Percentil")
    ax.set_ylabel("Latência (s)")
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)

    # 1.3 Tabela resumo
    ax = axes[2]
    ax.axis("off")
    rows = [
        ["Tempo total",          f"{metrics.get('total_time_s', 0):.2f} s"],
        ["Registros processados", f"{metrics.get('total_records', 0):,}"],
        ["Throughput",           f"{metrics.get('throughput_rps', 0):,.0f} rec/s"],
        ["Janelas (60s)",        f"{metrics.get('windows_processed', 0):,}"],
        ["Latência P50",         f"{metrics.get('latency_p50', 0) / 1000:.1f} s"],
        ["Latência P95",         f"{metrics.get('latency_p95', 0) / 1000:.1f} s"],
        ["Latência P99",         f"{metrics.get('latency_p99', 0) / 1000:.1f} s"],
        ["Latência média",       f"{metrics.get('latency_mean', 0) / 1000:.1f} s"],
    ]
    table = ax.table(
        cellText=rows,
        colLabels=["Métrica", "Valor"],
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.7)
    table[(0, 0)].set_facecolor(COLOR)
    table[(0, 0)].set_text_props(color="white", fontweight="bold")
    table[(0, 1)].set_facecolor(COLOR)
    table[(0, 1)].set_text_props(color="white", fontweight="bold")
    for i in range(1, len(rows) + 1):
        if i % 2 == 0:
            for j in range(2):
                table[(i, j)].set_facecolor("#f5f5f5")
    ax.set_title("Resumo", fontsize=10, pad=15)

    plt.tight_layout()
    p1 = output_dir / "spark_overview.png"
    plt.savefig(p1, dpi=150, bbox_inches="tight")
    print(f"  Painel geral salvo: {p1}")
    plt.close()

    # ------------------------------------------------------------------
    # Figura 2: CDF da latência
    # ------------------------------------------------------------------
    latencies = sorted(metrics.get("latency_ms_sample", []))
    if latencies:
        fig, ax = plt.subplots(figsize=(8, 5))
        cdf = np.arange(1, len(latencies) + 1) / len(latencies)
        ax.plot([l / 1000 for l in latencies], cdf, color=COLOR, linewidth=2)
        for pct, label in [(0.50, "P50"), (0.95, "P95"), (0.99, "P99")]:
            ax.axhline(pct, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
            ax.text(ax.get_xlim()[0], pct + 0.01, label, fontsize=8, color="gray")
        ax.set_title("CDF da Latência — Spark Structured Streaming", fontweight="bold")
        ax.set_xlabel("Latência (s)")
        ax.set_ylabel("Probabilidade acumulada")
        ax.grid(alpha=0.3)
        ax.spines[["top", "right"]].set_visible(False)
        plt.tight_layout()
        p2 = output_dir / "spark_latency_cdf.png"
        plt.savefig(p2, dpi=150, bbox_inches="tight")
        print(f"  CDF de latência salvo: {p2}")
        plt.close()

    # ------------------------------------------------------------------
    # Figura 3: Distribuição do tamanho das janelas
    # ------------------------------------------------------------------
    window_sizes = metrics.get("window_sizes", [])
    if window_sizes:
        ws = np.array(window_sizes)
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.hist(ws, bins=min(60, len(set(ws))), color=COLOR, edgecolor="white", alpha=0.85)
        ax.axvline(np.mean(ws), color="black", linestyle="--", linewidth=1.3,
                   label=f"Média: {np.mean(ws):.1f} reg/janela")
        ax.axvline(np.median(ws), color="#555", linestyle=":", linewidth=1.3,
                   label=f"Mediana: {int(np.median(ws))} reg/janela")
        ax.set_title(
            f"Distribuição do Tamanho das Janelas (60 s) — {len(ws):,} janelas",
            fontweight="bold",
        )
        ax.set_xlabel("Registros por janela")
        ax.set_ylabel("Frequência")
        ax.legend(fontsize=9)
        ax.grid(axis="y", alpha=0.3)
        ax.spines[["top", "right"]].set_visible(False)
        stats_text = (
            f"Min:  {ws.min()}\n"
            f"Max:  {ws.max()}\n"
            f"P50:  {int(np.percentile(ws, 50))}\n"
            f"P95:  {int(np.percentile(ws, 95))}\n"
            f"P99:  {int(np.percentile(ws, 99))}"
        )
        ax.text(0.97, 0.97, stats_text, transform=ax.transAxes,
                fontsize=9, va="top", ha="right",
                bbox=dict(boxstyle="round,pad=0.4", facecolor="white", alpha=0.85))
        plt.tight_layout()
        p3 = output_dir / "spark_window_sizes.png"
        plt.savefig(p3, dpi=150, bbox_inches="tight")
        print(f"  Distribuição de janelas salvo: {p3}")
        plt.close()

    print(f"\nFiguras geradas em: {output_dir}/")


def _print_metrics_table(metrics: dict) -> None:
    keys = [
        ("total_time_s",      "Tempo total (s)"),
        ("total_records",     "Total registros"),
        ("throughput_rps",    "Throughput (rec/s)"),
        ("windows_processed", "Janelas processadas"),
        ("latency_p50",       "Latência P50 (ms)"),
        ("latency_p95",       "Latência P95 (ms)"),
        ("latency_p99",       "Latência P99 (ms)"),
        ("latency_mean",      "Latência média (ms)"),
    ]
    print(f"{'Métrica':<30} {'Valor':>15}")
    print("-" * 47)
    for key, label in keys:
        val = metrics.get(key, "N/A")
        if isinstance(val, float):
            print(f"{label:<30} {val:>15.2f}")
        elif isinstance(val, int):
            print(f"{label:<30} {val:>15,}")
        else:
            print(f"{label:<30} {str(val):>15}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plota métricas do benchmark Spark — dataset 3W"
    )
    parser.add_argument(
        "--results",
        default=str(DEFAULT_RESULTS_DIR),
        help=f"Diretório com o JSON de métricas (default: {DEFAULT_RESULTS_DIR})",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Diretório para salvar as figuras (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    results_dir = Path(args.results)
    output_dir  = Path(args.output)

    try:
        metrics = load_metrics(results_dir)
    except FileNotFoundError as e:
        print(f"[ERRO] {e}")
        return

    print(f"Gerando plots em: {output_dir}\n")
    plot_all(metrics, output_dir)


if __name__ == "__main__":
    main()
