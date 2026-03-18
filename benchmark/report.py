"""
Geração de relatórios de comparação Flink vs Spark Structured Streaming.

Carrega os arquivos JSONL de resultados brutos, calcula sumários estatísticos
por cenário/engine e produz relatórios em JSON, CSV e tabela formatada no
terminal via ``rich``.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import pathlib
import sys
from typing import Any

import numpy as np
import structlog

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Carregamento de resultados brutos
# ---------------------------------------------------------------------------

def load_results(results_dir: str) -> dict[str, dict[str, list[dict]]]:
    """Carrega todos os arquivos JSONL de ``results/raw/`` em estrutura organizada.

    A estrutura esperada no disco é::

        results_dir/
          <scenario>/
            <engine>/
              metrics.jsonl

    Onde cada linha do JSONL é um snapshot de métricas coletado pelo
    :class:`benchmark.metrics_collector.MetricsCollector`.

    Parâmetros
    ----------
    results_dir:
        Caminho do diretório raiz de resultados brutos
        (ex: ``"results/raw"``).

    Retorna
    -------
    dict[str, dict[str, list[dict]]]
        Estrutura ``{scenario_name: {engine: [snapshot, ...]}}`` onde cada
        snapshot é o dicionário JSON de um único instante de coleta.
    """
    root = pathlib.Path(results_dir)
    if not root.exists():
        raise FileNotFoundError(f"Diretório de resultados não encontrado: {root}")

    results: dict[str, dict[str, list[dict]]] = {}
    jsonl_files_found = 0

    for scenario_dir in sorted(root.iterdir()):
        if not scenario_dir.is_dir():
            continue
        scenario_name = scenario_dir.name
        results[scenario_name] = {}

        for engine_dir in sorted(scenario_dir.iterdir()):
            if not engine_dir.is_dir():
                continue
            engine_name = engine_dir.name
            snapshots: list[dict] = []

            for jsonl_file in sorted(engine_dir.glob("*.jsonl")):
                jsonl_files_found += 1
                with open(jsonl_file, encoding="utf-8") as fh:
                    for line_num, line in enumerate(fh, start=1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            snapshots.append(json.loads(line))
                        except json.JSONDecodeError as exc:
                            log.warning(
                                "linha_jsonl_inválida",
                                file=str(jsonl_file),
                                line=line_num,
                                error=str(exc),
                            )

            results[scenario_name][engine_name] = snapshots

    log.info(
        "resultados_carregados",
        cenários=len(results),
        arquivos_jsonl=jsonl_files_found,
    )
    return results


# ---------------------------------------------------------------------------
# Sumário estatístico por cenário/engine
# ---------------------------------------------------------------------------

def _extract_latency_samples(snapshots: list[dict]) -> list[float]:
    """Extrai amostras de latência fim-a-fim dos snapshots.

    Tenta extrair ``e2e_latency_ms`` de múltiplas origens possíveis nos
    snapshots: campo direto, métricas Flink ou métricas Spark.

    Parâmetros
    ----------
    snapshots:
        Lista de snapshots JSONL.

    Retorna
    -------
    list[float]
        Amostras de latência em milissegundos (valores inválidos excluídos).
    """
    samples: list[float] = []
    for snap in snapshots:
        # Tentativa 1: campo direto no snapshot
        val = snap.get("e2e_latency_ms")
        if val is not None:
            try:
                samples.append(float(val))
                continue
            except (ValueError, TypeError):
                pass

        # Tentativa 2: latência da Flink REST API
        flink = snap.get("flink", {}) or {}
        for key in ("latency_p50", "latency_p95", "latency_p99"):
            v = flink.get(key)
            if v is not None:
                try:
                    samples.append(float(v))
                    break
                except (ValueError, TypeError):
                    pass

        # Tentativa 3: latência do Spark UI (trigger execution latency)
        spark = snap.get("spark", {}) or {}
        v = spark.get("trigger_execution_latency_ms")
        if v is not None:
            try:
                samples.append(float(v))
            except (ValueError, TypeError):
                pass

    return samples


def _extract_throughput_samples(snapshots: list[dict]) -> list[float]:
    """Extrai amostras de throughput (records/s) dos snapshots.

    Parâmetros
    ----------
    snapshots:
        Lista de snapshots JSONL.

    Retorna
    -------
    list[float]
        Amostras de throughput em records/s.
    """
    samples: list[float] = []
    for snap in snapshots:
        flink = snap.get("flink", {}) or {}
        spark = snap.get("spark", {}) or {}

        val = flink.get("records_in_per_second") or spark.get("input_rows_per_second")
        if val is not None:
            try:
                samples.append(float(val))
            except (ValueError, TypeError):
                pass

    return samples


def _extract_lag_samples(snapshots: list[dict]) -> list[float]:
    """Extrai amostras de consumer lag total dos snapshots.

    Parâmetros
    ----------
    snapshots:
        Lista de snapshots JSONL.

    Retorna
    -------
    list[float]
        Amostras de lag total (soma de todas as partições).
    """
    samples: list[float] = []
    for snap in snapshots:
        lag_by_partition: dict = snap.get("consumer_lag", {}) or {}
        if lag_by_partition:
            total = sum(
                float(v) for v in lag_by_partition.values() if v is not None
            )
            samples.append(total)
    return samples


def compute_scenario_summary(snapshots: list[dict]) -> dict[str, Any]:
    """Calcula sumário estatístico de um cenário a partir dos seus snapshots.

    Parâmetros
    ----------
    snapshots:
        Lista de snapshots JSONL coletados durante a fase de medição
        (após warm-up descartado).

    Retorna
    -------
    dict[str, Any]
        Sumário com:

        - ``snapshot_count``: número de snapshots processados.
        - ``e2e_latency_ms``: subdict com p50, p95, p99 e count.
        - ``throughput_records_per_s``: subdict com mean e max.
        - ``consumer_lag``: subdict com mean e max do lag total.
        - ``recovery_time_s``: tempo de recuperação em segundos (cenários
          de fault), ou ``None``.
    """
    latency_samples = _extract_latency_samples(snapshots)
    throughput_samples = _extract_throughput_samples(snapshots)
    lag_samples = _extract_lag_samples(snapshots)

    # Latência
    latency_summary: dict[str, Any]
    if latency_samples:
        arr = np.array(latency_samples, dtype=np.float64)
        latency_summary = {
            "p50": float(np.percentile(arr, 50)),
            "p95": float(np.percentile(arr, 95)),
            "p99": float(np.percentile(arr, 99)),
            "mean": float(np.mean(arr)),
            "count": len(latency_samples),
        }
    else:
        latency_summary = {"p50": None, "p95": None, "p99": None, "mean": None, "count": 0}

    # Throughput
    throughput_summary: dict[str, Any]
    if throughput_samples:
        arr_t = np.array(throughput_samples, dtype=np.float64)
        throughput_summary = {
            "mean": float(np.mean(arr_t)),
            "max": float(np.max(arr_t)),
            "count": len(throughput_samples),
        }
    else:
        throughput_summary = {"mean": None, "max": None, "count": 0}

    # Consumer lag
    lag_summary: dict[str, Any]
    if lag_samples:
        arr_l = np.array(lag_samples, dtype=np.float64)
        lag_summary = {
            "mean": float(np.mean(arr_l)),
            "max": float(np.max(arr_l)),
            "count": len(lag_samples),
        }
    else:
        lag_summary = {"mean": None, "max": None, "count": 0}

    # Tempo de recuperação (cenário de fault)
    recovery_time_s: float | None = None
    for snap in snapshots:
        rt = snap.get("recovery_time_s")
        if rt is not None:
            try:
                recovery_time_s = float(rt)
                break
            except (ValueError, TypeError):
                pass

    return {
        "snapshot_count": len(snapshots),
        "e2e_latency_ms": latency_summary,
        "throughput_records_per_s": throughput_summary,
        "consumer_lag": lag_summary,
        "recovery_time_s": recovery_time_s,
    }


# ---------------------------------------------------------------------------
# Relatório de comparação completo
# ---------------------------------------------------------------------------

def generate_comparison_report(
    results_dir: str,
    output_path: str,
) -> dict[str, Any]:
    """Gera relatório de comparação Flink vs Spark em JSON e CSV.

    Para cada cenário encontrado nos resultados brutos, calcula o sumário
    estatístico para cada engine e os organiza lado a lado para comparação.

    Parâmetros
    ----------
    results_dir:
        Caminho do diretório de resultados brutos (``results/raw/``).
    output_path:
        Caminho do arquivo JSON de saída do relatório completo.

    Retorna
    -------
    dict[str, Any]
        Relatório estruturado com:

        - ``generated_at``: timestamp ISO 8601 de geração.
        - ``scenarios``: dicionário ``{scenario: {engine: summary}}``.
        - ``comparison_table``: lista de registros para exportação CSV.

    Side effects
    ------------
    - Salva o relatório JSON em ``output_path``.
    - Salva uma tabela CSV em ``output_path.replace('.json', '.csv')``.
    """
    from datetime import datetime, timezone

    all_results = load_results(results_dir)

    report: dict[str, Any] = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "results_dir": str(pathlib.Path(results_dir).resolve()),
        "scenarios": {},
        "comparison_table": [],
    }

    csv_rows: list[dict] = []

    for scenario_name, engines_data in all_results.items():
        scenario_report: dict[str, Any] = {}

        for engine_name, snapshots in engines_data.items():
            if not snapshots:
                log.warning(
                    "sem_snapshots",
                    scenario=scenario_name,
                    engine=engine_name,
                )
                scenario_report[engine_name] = None
                continue

            summary = compute_scenario_summary(snapshots)
            scenario_report[engine_name] = summary

        report["scenarios"][scenario_name] = scenario_report

        # Monta linha da tabela comparativa CSV
        flink_s = scenario_report.get("flink") or {}
        spark_s = scenario_report.get("spark") or {}

        def _get(d: dict, *keys: str) -> Any:
            """Navega hierarquicamente em um dicionário aninhado."""
            for k in keys:
                if not isinstance(d, dict):
                    return None
                d = d.get(k)  # type: ignore[assignment]
            return d

        csv_row = {
            "scenario": scenario_name,
            "flink_latency_p50_ms": _get(flink_s, "e2e_latency_ms", "p50"),
            "flink_latency_p95_ms": _get(flink_s, "e2e_latency_ms", "p95"),
            "flink_latency_p99_ms": _get(flink_s, "e2e_latency_ms", "p99"),
            "flink_throughput_mean": _get(flink_s, "throughput_records_per_s", "mean"),
            "flink_throughput_max": _get(flink_s, "throughput_records_per_s", "max"),
            "flink_lag_mean": _get(flink_s, "consumer_lag", "mean"),
            "flink_recovery_time_s": _get(flink_s, "recovery_time_s"),
            "spark_latency_p50_ms": _get(spark_s, "e2e_latency_ms", "p50"),
            "spark_latency_p95_ms": _get(spark_s, "e2e_latency_ms", "p95"),
            "spark_latency_p99_ms": _get(spark_s, "e2e_latency_ms", "p99"),
            "spark_throughput_mean": _get(spark_s, "throughput_records_per_s", "mean"),
            "spark_throughput_max": _get(spark_s, "throughput_records_per_s", "max"),
            "spark_lag_mean": _get(spark_s, "consumer_lag", "mean"),
            "spark_recovery_time_s": _get(spark_s, "recovery_time_s"),
        }
        csv_rows.append(csv_row)

    report["comparison_table"] = csv_rows

    # Salva JSON
    output_p = pathlib.Path(output_path)
    output_p.parent.mkdir(parents=True, exist_ok=True)
    with open(output_p, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    log.info("relatório_json_salvo", path=str(output_p))

    # Salva CSV
    csv_path = output_p.with_suffix(".csv")
    if csv_rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=csv_rows[0].keys())
            writer.writeheader()
            writer.writerows(csv_rows)
        log.info("relatório_csv_salvo", path=str(csv_path))

    return report


# ---------------------------------------------------------------------------
# Impressão formatada no terminal
# ---------------------------------------------------------------------------

def print_summary_table(report: dict[str, Any]) -> None:
    """Imprime tabela formatada de comparação Flink vs Spark no terminal.

    Usa a biblioteca ``rich`` para formatação colorida. As colunas exibidas
    são: cenário, latência p50/p95/p99 (ms) e throughput médio (records/s)
    para cada engine.

    Parâmetros
    ----------
    report:
        Dicionário retornado por :func:`generate_comparison_report`.
    """
    try:
        from rich.console import Console
        from rich.table import Table
        from rich import box
    except ImportError:
        # Fallback sem rich
        _print_plain_table(report)
        return

    console = Console()
    table = Table(
        title="Benchmark 3W — Flink vs Spark Structured Streaming",
        box=box.ROUNDED,
        show_lines=True,
        highlight=True,
    )

    # Cabeçalho
    table.add_column("Cenário", style="bold cyan", no_wrap=True)
    table.add_column("Flink p50 (ms)", justify="right", style="green")
    table.add_column("Flink p95 (ms)", justify="right", style="green")
    table.add_column("Flink p99 (ms)", justify="right", style="green")
    table.add_column("Flink tput (r/s)", justify="right", style="green")
    table.add_column("Spark p50 (ms)", justify="right", style="yellow")
    table.add_column("Spark p95 (ms)", justify="right", style="yellow")
    table.add_column("Spark p99 (ms)", justify="right", style="yellow")
    table.add_column("Spark tput (r/s)", justify="right", style="yellow")
    table.add_column("Recovery Flink (s)", justify="right", style="magenta")
    table.add_column("Recovery Spark (s)", justify="right", style="magenta")

    def _fmt(val: Any, decimals: int = 1) -> str:
        """Formata valor numérico ou retorna traço se ausente."""
        if val is None:
            return "[dim]-[/dim]"
        try:
            return f"{float(val):.{decimals}f}"
        except (ValueError, TypeError):
            return str(val)

    for row in report.get("comparison_table", []):
        table.add_row(
            row["scenario"],
            _fmt(row["flink_latency_p50_ms"]),
            _fmt(row["flink_latency_p95_ms"]),
            _fmt(row["flink_latency_p99_ms"]),
            _fmt(row["flink_throughput_mean"], decimals=0),
            _fmt(row["spark_latency_p50_ms"]),
            _fmt(row["spark_latency_p95_ms"]),
            _fmt(row["spark_latency_p99_ms"]),
            _fmt(row["spark_throughput_mean"], decimals=0),
            _fmt(row["flink_recovery_time_s"]),
            _fmt(row["spark_recovery_time_s"]),
        )

    console.print(table)
    console.print(
        f"\n[dim]Gerado em: {report.get('generated_at', 'N/A')}[/dim]\n"
    )


def _print_plain_table(report: dict[str, Any]) -> None:
    """Fallback de impressão de tabela sem rich (texto puro).

    Parâmetros
    ----------
    report:
        Dicionário retornado por :func:`generate_comparison_report`.
    """
    print(
        f"\n{'CENÁRIO':<20} "
        f"{'F_p50':>8} {'F_p95':>8} {'F_p99':>8} {'F_tput':>8} "
        f"{'S_p50':>8} {'S_p95':>8} {'S_p99':>8} {'S_tput':>8} "
        f"{'F_rec':>8} {'S_rec':>8}"
    )
    print("-" * 110)

    def _v(val: Any, dec: int = 1) -> str:
        if val is None:
            return "-"
        try:
            return f"{float(val):.{dec}f}"
        except (ValueError, TypeError):
            return str(val)

    for row in report.get("comparison_table", []):
        print(
            f"{row['scenario']:<20} "
            f"{_v(row['flink_latency_p50_ms']):>8} "
            f"{_v(row['flink_latency_p95_ms']):>8} "
            f"{_v(row['flink_latency_p99_ms']):>8} "
            f"{_v(row['flink_throughput_mean'], 0):>8} "
            f"{_v(row['spark_latency_p50_ms']):>8} "
            f"{_v(row['spark_latency_p95_ms']):>8} "
            f"{_v(row['spark_latency_p99_ms']):>8} "
            f"{_v(row['spark_throughput_mean'], 0):>8} "
            f"{_v(row['flink_recovery_time_s']):>8} "
            f"{_v(row['spark_recovery_time_s']):>8}"
        )
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    """Constrói o parser de argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description="Geração de relatórios do benchmark 3W Flink vs Spark.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Gerar relatório JSON + CSV e imprimir tabela
  python -m benchmark.report --results-dir results/raw --output results/summary.json

  # Apenas imprimir tabela de um relatório já gerado
  python -m benchmark.report --results-dir results/raw --output results/summary.json --format table

  # Apenas CSV
  python -m benchmark.report --results-dir results/raw --output results/summary.json --format csv
""",
    )
    parser.add_argument(
        "--results-dir",
        default="results/raw",
        metavar="DIR",
        help="Diretório com resultados brutos (padrão: results/raw).",
    )
    parser.add_argument(
        "--output",
        default="results/summary.json",
        metavar="PATH",
        help="Caminho do arquivo JSON de saída (padrão: results/summary.json).",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv", "table", "all"],
        default="all",
        help=(
            "Formato de saída: json (apenas arquivo JSON), csv (apenas CSV), "
            "table (apenas tabela terminal), all (todos — padrão)."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada principal da CLI de relatórios."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        report = generate_comparison_report(
            results_dir=args.results_dir,
            output_path=args.output,
        )
    except FileNotFoundError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 1

    if args.format in ("table", "all"):
        print_summary_table(report)

    if args.format == "json":
        print(f"Relatório JSON salvo em: {args.output}")
    elif args.format == "csv":
        csv_path = str(pathlib.Path(args.output).with_suffix(".csv"))
        print(f"Relatório CSV salvo em: {csv_path}")
    elif args.format == "all":
        csv_path = str(pathlib.Path(args.output).with_suffix(".csv"))
        print(f"Relatório JSON: {args.output}")
        print(f"Relatório CSV:  {csv_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
