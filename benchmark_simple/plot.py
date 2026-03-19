#!/usr/bin/env python3
"""
Plota métricas de latência e throughput do benchmark Spark vs Flink.

Lê os JSON gerados por run.py e gera figuras comparativas.

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
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "plots"


def load_metrics(results_dir: Path) -> dict:
    """Carrega métricas do diretório de resultados."""
    metrics = {}

    summary_file = results_dir / "summary.json"
    if summary_file.exists():
        with open(summary_file) as f:
            data = json.load(f)
        for engine in ("spark", "flink"):
            if engine in data:
                metrics[engine] = data[engine]
        return metrics

    # Fallback: lê arquivos individuais
    for engine in ("spark", "flink"):
        mfile = results_dir / f"{engine}_metrics.json"
        if mfile.exists():
            with open(mfile) as f:
                metrics[engine] = json.load(f)

    return metrics


def plot_all(metrics: dict, output_dir: Path) -> None:
    """Gera todas as figuras de comparação."""
    try:
        import matplotlib
        matplotlib.use("Agg")  # sem display — salva direto em arquivo
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
    except ImportError:
        print("[ERRO] matplotlib não encontrado. Instale: pip install matplotlib")
        print("\nImprimindo métricas no terminal como alternativa:\n")
        _print_metrics_table(metrics)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    engines = [e for e in ("spark", "flink") if e in metrics]
    colors = {"spark": "#E25A1C", "flink": "#4B9CD3"}  # laranja Spark, azul Flink

    if not engines:
        print("[ERRO] Nenhuma métrica encontrada para plotar.")
        return

    # ------------------------------------------------------------------
    # Figura 1: Painel completo (2×2)
    # ------------------------------------------------------------------
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Benchmark: Spark vs Flink — Dataset 3W (Petrobras)", fontsize=14, fontweight="bold")

    # --- 1.1 Throughput (bar chart) ---
    ax = axes[0, 0]
    throughputs = [metrics[e].get("throughput_rps", 0) for e in engines]
    bars = ax.bar(engines, throughputs, color=[colors[e] for e in engines], width=0.5, edgecolor="white")
    ax.set_title("Throughput")
    ax.set_ylabel("Registros / segundo")
    ax.set_xlabel("Engine")
    for bar, val in zip(bars, throughputs):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(throughputs) * 0.01,
                f"{val:,.0f}", ha="center", va="bottom", fontsize=10)
    ax.set_ylim(0, max(throughputs) * 1.2 if throughputs else 1)
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)

    # --- 1.2 Latência por percentil (grouped bar chart) ---
    ax = axes[0, 1]
    percentiles = ["p50", "p95", "p99"]
    x = np.arange(len(percentiles))
    width = 0.35
    for i, engine in enumerate(engines):
        vals = [metrics[engine].get(f"latency_{p}", 0) for p in percentiles]
        offset = (i - len(engines) / 2 + 0.5) * width
        rects = ax.bar(x + offset, vals, width, label=engine.capitalize(),
                       color=colors[engine], edgecolor="white", alpha=0.9)
        for rect, val in zip(rects, vals):
            ax.text(rect.get_x() + rect.get_width() / 2, rect.get_height() + 1,
                    f"{val:.0f}", ha="center", va="bottom", fontsize=8)
    ax.set_title("Latência por Percentil")
    ax.set_ylabel("Latência (ms)")
    ax.set_xticks(x)
    ax.set_xticklabels(["P50", "P95", "P99"])
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)

    # --- 1.3 Distribuição de latência (histograma) ---
    ax = axes[1, 0]
    has_latency_data = False
    for engine in engines:
        latencies = metrics[engine].get("latency_ms_sample", [])
        if latencies:
            ax.hist(latencies, bins=40, alpha=0.6, label=engine.capitalize(),
                    color=colors[engine], edgecolor="none", density=True)
            has_latency_data = True
    if has_latency_data:
        ax.set_title("Distribuição de Latência")
        ax.set_xlabel("Latência (ms)")
        ax.set_ylabel("Densidade")
        ax.legend()
        ax.grid(alpha=0.3)
        ax.spines[["top", "right"]].set_visible(False)
    else:
        ax.text(0.5, 0.5, "Sem dados de latência\ndisponíveis",
                ha="center", va="center", transform=ax.transAxes, fontsize=11, color="gray")
        ax.set_title("Distribuição de Latência")

    # --- 1.4 Tempo total e registros processados (tabela resumo) ---
    ax = axes[1, 1]
    ax.axis("off")
    table_data = []
    row_labels = [
        "Tempo total (s)",
        "Total registros",
        "Throughput (rec/s)",
        "Latência P50 (ms)",
        "Latência P95 (ms)",
        "Latência P99 (ms)",
        "Latência média (ms)",
    ]
    col_labels = ["Métrica"] + [e.capitalize() for e in engines]
    keys = ["total_time_s", "total_records", "throughput_rps",
            "latency_p50", "latency_p95", "latency_p99", "latency_mean"]

    for label, key in zip(row_labels, keys):
        row = [label]
        for engine in engines:
            val = metrics[engine].get(key, "N/A")
            if isinstance(val, float):
                row.append(f"{val:,.2f}")
            elif isinstance(val, int):
                row.append(f"{val:,}")
            else:
                row.append(str(val))
        table_data.append(row)

    table = ax.table(
        cellText=table_data,
        colLabels=col_labels,
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.6)

    # Colorir cabeçalhos
    for j, engine in enumerate(engines, start=1):
        table[(0, j)].set_facecolor(colors[engine])
        table[(0, j)].set_text_props(color="white", fontweight="bold")
    table[(0, 0)].set_facecolor("#333333")
    table[(0, 0)].set_text_props(color="white", fontweight="bold")

    # Linhas alternadas
    for i in range(1, len(row_labels) + 1):
        for j in range(len(col_labels)):
            if i % 2 == 0:
                table[(i, j)].set_facecolor("#f5f5f5")

    ax.set_title("Resumo Comparativo", fontsize=10, pad=20)

    plt.tight_layout()
    out_path = output_dir / "benchmark_overview.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"  Painel geral salvo: {out_path}")
    plt.close()

    # ------------------------------------------------------------------
    # Figura 2: Throughput ao longo do tempo (usando latency_ms_sample
    #           como proxy de tempo de processamento por lote)
    # ------------------------------------------------------------------
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Latência e Throughput — Spark vs Flink", fontsize=13, fontweight="bold")

    # --- 2.1 Latências acumuladas (CDF) ---
    ax = axes[0]
    for engine in engines:
        latencies = sorted(metrics[engine].get("latency_ms_sample", []))
        if latencies:
            cdf = np.arange(1, len(latencies) + 1) / len(latencies)
            ax.plot(latencies, cdf, label=engine.capitalize(),
                    color=colors[engine], linewidth=2)
    ax.set_title("CDF da Latência")
    ax.set_xlabel("Latência (ms)")
    ax.set_ylabel("Probabilidade acumulada")
    ax.axhline(0.50, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
    ax.axhline(0.95, color="gray", linestyle=":", linewidth=0.8, alpha=0.7)
    ax.axhline(0.99, color="gray", linestyle="-.", linewidth=0.8, alpha=0.7)
    ax.text(ax.get_xlim()[1] * 0.02 if ax.get_xlim()[1] > 0 else 1,
            0.51, "P50", fontsize=8, color="gray")
    ax.text(ax.get_xlim()[1] * 0.02 if ax.get_xlim()[1] > 0 else 1,
            0.96, "P95", fontsize=8, color="gray")
    ax.text(ax.get_xlim()[1] * 0.02 if ax.get_xlim()[1] > 0 else 1,
            1.00, "P99", fontsize=8, color="gray")
    ax.legend()
    ax.grid(alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)

    # --- 2.2 Comparação de throughput + tempo total (dual axis) ---
    ax = axes[1]
    ax2 = ax.twinx()

    x_pos = np.arange(len(engines))
    width = 0.4

    throughputs = [metrics[e].get("throughput_rps", 0) for e in engines]
    times = [metrics[e].get("total_time_s", 0) for e in engines]

    bars1 = ax.bar(x_pos - width / 2, throughputs, width, label="Throughput (rec/s)",
                   color=[colors[e] for e in engines], alpha=0.85, edgecolor="white")
    bars2 = ax2.bar(x_pos + width / 2, times, width, label="Tempo total (s)",
                    color=[colors[e] for e in engines], alpha=0.45, edgecolor="white",
                    hatch="///")

    for bar, val in zip(bars1, throughputs):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(throughputs) * 0.01,
                f"{val:,.0f}", ha="center", va="bottom", fontsize=9)
    for bar, val in zip(bars2, times):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(times) * 0.01,
                 f"{val:.1f}s", ha="center", va="bottom", fontsize=9, style="italic")

    ax.set_title("Throughput e Tempo Total")
    ax.set_ylabel("Throughput (registros/s)", color="black")
    ax2.set_ylabel("Tempo total (s)", color="gray")
    ax.set_xticks(x_pos)
    ax.set_xticklabels([e.capitalize() for e in engines])
    ax.set_ylim(0, max(throughputs) * 1.25 if throughputs else 1)
    ax2.set_ylim(0, max(times) * 1.25 if times else 1)
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top"]].set_visible(False)

    # Legenda manual
    patch1 = mpatches.Patch(color="gray", alpha=0.85, label="Throughput (rec/s)")
    patch2 = mpatches.Patch(color="gray", alpha=0.45, hatch="///", label="Tempo total (s)")
    ax.legend(handles=[patch1, patch2], loc="upper right", fontsize=8)

    plt.tight_layout()
    out_path2 = output_dir / "benchmark_latency_throughput.png"
    plt.savefig(out_path2, dpi=150, bbox_inches="tight")
    print(f"  Latência e throughput salvo: {out_path2}")
    plt.close()

    print(f"\nFiguras geradas em: {output_dir}/")


def _print_metrics_table(metrics: dict) -> None:
    """Imprime métricas no terminal quando matplotlib não está disponível."""
    engines = list(metrics.keys())
    keys = [
        ("total_time_s",    "Tempo total (s)"),
        ("total_records",   "Total registros"),
        ("throughput_rps",  "Throughput (rec/s)"),
        ("latency_p50",     "Latência P50 (ms)"),
        ("latency_p95",     "Latência P95 (ms)"),
        ("latency_p99",     "Latência P99 (ms)"),
        ("latency_mean",    "Latência média (ms)"),
    ]

    col_w = 20
    header = f"{'Métrica':<30}" + "".join(f"{e.capitalize():>{col_w}}" for e in engines)
    print(header)
    print("-" * len(header))
    for key, label in keys:
        row = f"{label:<30}"
        for e in engines:
            val = metrics[e].get(key, "N/A")
            if isinstance(val, float):
                row += f"{val:>{col_w}.2f}"
            elif isinstance(val, int):
                row += f"{val:>{col_w},}"
            else:
                row += f"{str(val):>{col_w}}"
        print(row)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plota métricas de latência e throughput do benchmark Spark vs Flink"
    )
    parser.add_argument(
        "--results",
        default=str(DEFAULT_RESULTS_DIR),
        help=f"Diretório com os JSON de métricas (default: {DEFAULT_RESULTS_DIR})",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Diretório para salvar as figuras (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    results_dir = Path(args.results)
    output_dir = Path(args.output)

    if not results_dir.exists():
        print(f"[ERRO] Diretório de resultados não encontrado: {results_dir}")
        print("Execute primeiro: python benchmark_simple/run.py")
        return

    metrics = load_metrics(results_dir)
    if not metrics:
        print(f"[ERRO] Nenhuma métrica encontrada em {results_dir}")
        print("Execute primeiro: python benchmark_simple/run.py")
        return

    print(f"Engines encontrados: {', '.join(metrics.keys())}")
    print(f"Gerando plots em: {output_dir}\n")

    plot_all(metrics, output_dir)


if __name__ == "__main__":
    main()
