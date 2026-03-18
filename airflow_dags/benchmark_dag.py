"""
DAG Airflow para orquestração completa do benchmark 3W: Flink vs Spark SS.

Cada cenário definido em ``benchmark.scenarios`` é executado sequencialmente
para ambos os engines (Flink e Spark), garantindo que os dois nunca disputem
recursos simultaneamente — condição essencial para comparação justa.

Fluxo por cenário × engine:
    setup_topic → start_engine → start_producer → warmup_wait →
    start_metrics → run_experiment → stop_metrics →
    stop_producer → stop_engine → collect_results → generate_report
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# Configuração padrão da DAG
# ---------------------------------------------------------------------------

_DEFAULT_ARGS: dict[str, Any] = {
    "owner": "benchmark",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Variáveis de ambiente com fallbacks seguros
_KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
_KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "3w-sensors")
_PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
_PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
_FLINK_JM_HOST = os.getenv("FLINK_JM_HOST", "localhost")
_FLINK_JM_PORT = int(os.getenv("FLINK_JM_PORT", "8081"))
_RESULTS_PATH = os.getenv("RESULTS_PATH", "./results")
_COMPOSE_PROJECT = os.getenv("COMPOSE_PROJECT_NAME", "benchmark3w")
_DATASET_PATH = os.getenv("DATASET_PATH", "/data/3w/dataset")

# Importa cenários definidos no módulo de benchmark
# Usando importação lazy para não quebrar se o módulo não estiver instalado
# no ambiente do scheduler Airflow
try:
    from benchmark.scenarios import SCENARIOS, Scenario  # type: ignore[import]
except ImportError:
    # Fallback mínimo para permitir parse da DAG sem o módulo instalado
    from dataclasses import dataclass, field

    @dataclass
    class Scenario:  # type: ignore[no-redef]
        """Stub de Scenario para parse da DAG sem o módulo benchmark."""
        name: str
        speed_factor: float
        workers: int
        partitions: int
        fault: dict | None = None
        duration_s: int = 600
        warmup_s: int = 180

    SCENARIOS = [
        Scenario("baseline_1x", 1.0, 2, 4),
        Scenario("load_5x", 5.0, 2, 4),
        Scenario("load_10x", 10.0, 2, 4),
        Scenario("scale_1w", 5.0, 1, 4),
        Scenario("scale_2w", 5.0, 2, 4),
        Scenario("scale_3w", 5.0, 3, 8),
        Scenario("scale_5w", 5.0, 5, 8),
        Scenario("partitions_4", 5.0, 3, 4),
        Scenario("partitions_8", 5.0, 3, 8),
        Scenario("partitions_16", 5.0, 3, 16),
        Scenario("partitions_32", 5.0, 3, 32),
        Scenario("fault_worker", 5.0, 3, 8,
                 fault={"type": "kill_worker", "target": 2, "after_seconds": 120}),
    ]


# ---------------------------------------------------------------------------
# Funções Python usadas como callables nos PythonOperators
# ---------------------------------------------------------------------------

def _setup_topic(scenario_name: str, partitions: int, **kwargs: Any) -> None:
    """(Re)cria o tópico Kafka com o número de partições do cenário.

    Parâmetros
    ----------
    scenario_name:
        Nome do cenário (para logging).
    partitions:
        Número de partições Kafka a configurar.
    """
    from benchmark.scenarios import setup_kafka_topic  # type: ignore[import]

    print(
        f"[{scenario_name}] Configurando tópico '{_KAFKA_TOPIC}' "
        f"com {partitions} partições ..."
    )
    setup_kafka_topic(
        bootstrap_servers=_KAFKA_BROKERS,
        topic=_KAFKA_TOPIC,
        partitions=partitions,
    )
    print(f"[{scenario_name}] Tópico configurado com sucesso.")


def _start_engine(scenario_name: str, engine: str, workers: int, **kwargs: Any) -> None:
    """Escala o serviço do engine via docker-compose para o número de workers.

    Parâmetros
    ----------
    scenario_name:
        Nome do cenário (para logging).
    engine:
        Engine a iniciar: ``"flink"`` ou ``"spark"``.
    workers:
        Número de réplicas de worker a escalar.
    """
    service_map = {
        "flink": "flink-taskmanager",
        "spark": "spark-worker",
    }
    service = service_map[engine]

    print(
        f"[{scenario_name}/{engine}] Escalando '{service}' para {workers} réplica(s) ..."
    )
    result = subprocess.run(
        [
            "docker-compose",
            "--project-name", _COMPOSE_PROJECT,
            "up", "-d",
            "--scale", f"{service}={workers}",
            "--no-recreate",
            service,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"docker-compose scale '{service}={workers}' falhou: {result.stderr}"
        )

    # Aguarda serviço estabilizar (healthcheck básico)
    time.sleep(10)
    print(f"[{scenario_name}/{engine}] Engine iniciado com {workers} worker(s).")


def _stop_engine(scenario_name: str, engine: str, **kwargs: Any) -> None:
    """Para o engine escalando workers para zero.

    Parâmetros
    ----------
    scenario_name:
        Nome do cenário.
    engine:
        Engine a parar: ``"flink"`` ou ``"spark"``.
    """
    service_map = {
        "flink": "flink-taskmanager",
        "spark": "spark-worker",
    }
    service = service_map[engine]

    print(f"[{scenario_name}/{engine}] Parando engine '{service}' ...")
    subprocess.run(
        [
            "docker-compose",
            "--project-name", _COMPOSE_PROJECT,
            "stop", service,
        ],
        capture_output=True,
        text=True,
    )
    print(f"[{scenario_name}/{engine}] Engine parado.")


def _start_metrics_collection(
    scenario_name: str,
    engine: str,
    **kwargs: Any,
) -> None:
    """Registra o início da coleta de métricas via XCom.

    O coletor de métricas efetivo roda no worker do engine (Flink/Spark).
    Esta task apenas sinaliza o início registrando o timestamp via XCom
    para cálculo posterior de janelas de coleta.

    Parâmetros
    ----------
    scenario_name:
        Nome do cenário.
    engine:
        Engine em execução.
    """
    ti = kwargs.get("ti")
    start_ts = time.time()
    if ti:
        ti.xcom_push(key="metrics_start_ts", value=start_ts)
    print(
        f"[{scenario_name}/{engine}] Coleta de métricas iniciada "
        f"(ts={start_ts:.0f})."
    )


def _stop_metrics_and_collect(
    scenario_name: str,
    engine: str,
    **kwargs: Any,
) -> None:
    """Para coleta e grava snapshot final de métricas do Prometheus.

    Parâmetros
    ----------
    scenario_name:
        Nome do cenário.
    engine:
        Engine em execução.
    """
    from benchmark.metrics_collector import MetricsCollector  # type: ignore[import]
    import pathlib

    ti = kwargs.get("ti")
    start_ts = ti.xcom_pull(key="metrics_start_ts") if ti else None

    collector = MetricsCollector(
        prometheus_url=_PROMETHEUS_URL,
        kafka_brokers=_KAFKA_BROKERS,
        flink_jm_host=_FLINK_JM_HOST,
        flink_jm_port=_FLINK_JM_PORT,
        pushgateway_url=_PUSHGATEWAY_URL,
    )

    # Snapshot final
    snapshot = {
        "collected_at": datetime.utcnow().isoformat() + "Z",
        "final": True,
        "start_ts": start_ts,
        "prometheus": collector.collect_prometheus_snapshot(),
        "consumer_lag": collector.collect_kafka_consumer_lag(
            group_id=f"{engine}-3w-consumer",
            topic=_KAFKA_TOPIC,
        ),
        "flink": collector.collect_flink_metrics() if engine == "flink" else {},
        "spark": collector.collect_spark_metrics() if engine == "spark" else {},
    }

    output_dir = pathlib.Path(_RESULTS_PATH) / "raw" / scenario_name / engine
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "final_snapshot.jsonl"

    with open(snapshot_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(snapshot, default=str) + "\n")

    print(
        f"[{scenario_name}/{engine}] Snapshot final salvo em: {snapshot_path}"
    )


def _generate_report(**kwargs: Any) -> None:
    """Gera o relatório consolidado de comparação após todos os cenários.

    Invoca :func:`benchmark.report.generate_comparison_report` e imprime
    a tabela de sumário no log do Airflow.
    """
    from benchmark.report import generate_comparison_report, print_summary_table  # type: ignore[import]

    output_path = os.path.join(_RESULTS_PATH, "summary.json")
    print(f"Gerando relatório de comparação em: {output_path}")

    report = generate_comparison_report(
        results_dir=os.path.join(_RESULTS_PATH, "raw"),
        output_path=output_path,
    )
    print_summary_table(report)
    print(f"Relatório gerado com sucesso: {output_path}")


# ---------------------------------------------------------------------------
# Construção da DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="benchmark_3w_pipeline",
    description=(
        "Orquestra o benchmark completo de Flink vs Spark Structured Streaming "
        "usando o dataset 3W da Petrobras."
    ),
    schedule_interval=None,          # Trigger manual apenas
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["benchmark", "3w", "petrobras", "streaming"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
) as dag:

    # Referências para encadeamento sequencial entre TaskGroups de cenários
    last_task = None

    for scenario in SCENARIOS:
        sn = scenario.name  # alias curto

        for engine in ("flink", "spark"):
            group_id = f"{sn}__{engine}"

            with TaskGroup(group_id=group_id, dag=dag) as tg:

                # 1. Configurar tópico Kafka
                t_setup_topic = PythonOperator(
                    task_id="setup_topic",
                    python_callable=_setup_topic,
                    op_kwargs={
                        "scenario_name": sn,
                        "partitions": scenario.partitions,
                    },
                )

                # 2. Iniciar engine
                t_start_engine = PythonOperator(
                    task_id="start_engine",
                    python_callable=_start_engine,
                    op_kwargs={
                        "scenario_name": sn,
                        "engine": engine,
                        "workers": scenario.workers,
                    },
                )

                # 3. Iniciar producer com speed_factor do cenário
                t_start_producer = BashOperator(
                    task_id="start_producer",
                    bash_command=(
                        "nohup python /opt/benchmark/producer/producer.py "
                        f"--speed-factor {scenario.speed_factor} "
                        f"--dataset-path {_DATASET_PATH} "
                        f"--bootstrap-servers {_KAFKA_BROKERS} "
                        f"--topic {_KAFKA_TOPIC} "
                        f"> /tmp/producer_{sn}_{engine}.log 2>&1 & "
                        f"echo $! > /tmp/producer_{sn}_{engine}.pid"
                    ),
                    env={
                        "KAFKA_BROKERS": _KAFKA_BROKERS,
                        "KAFKA_TOPIC": _KAFKA_TOPIC,
                        "DATASET_PATH": _DATASET_PATH,
                    },
                )

                # 4. Aguardar warm-up
                t_warmup = BashOperator(
                    task_id="warmup_wait",
                    bash_command=f"echo 'Warm-up de {scenario.warmup_s}s para {sn}/{engine}...' && sleep {scenario.warmup_s}",
                )

                # 5. Iniciar coleta de métricas
                t_start_metrics = PythonOperator(
                    task_id="start_metrics",
                    python_callable=_start_metrics_collection,
                    op_kwargs={
                        "scenario_name": sn,
                        "engine": engine,
                    },
                )

                # 6. Rodar experimento (duração = total - warmup)
                experiment_duration = scenario.duration_s - scenario.warmup_s
                t_run_experiment = BashOperator(
                    task_id="run_experiment",
                    bash_command=(
                        f"echo 'Experimento {sn}/{engine} por {experiment_duration}s ...' "
                        f"&& sleep {experiment_duration}"
                    ),
                )

                # 7. Parar coleta e salvar snapshot final
                t_stop_metrics = PythonOperator(
                    task_id="stop_metrics",
                    python_callable=_stop_metrics_and_collect,
                    op_kwargs={
                        "scenario_name": sn,
                        "engine": engine,
                    },
                )

                # 8. Parar producer
                t_stop_producer = BashOperator(
                    task_id="stop_producer",
                    bash_command=(
                        f"PID_FILE=/tmp/producer_{sn}_{engine}.pid; "
                        "if [ -f \"$PID_FILE\" ]; then "
                        "  PID=$(cat \"$PID_FILE\"); "
                        "  kill \"$PID\" 2>/dev/null && echo \"Producer PID=$PID parado.\"; "
                        "  rm -f \"$PID_FILE\"; "
                        "else "
                        "  echo 'PID file não encontrado, producer já parado.'; "
                        "fi"
                    ),
                    trigger_rule="all_done",  # Para mesmo se experimento falhou
                )

                # 9. Parar engine
                t_stop_engine = PythonOperator(
                    task_id="stop_engine",
                    python_callable=_stop_engine,
                    op_kwargs={
                        "scenario_name": sn,
                        "engine": engine,
                    },
                    trigger_rule="all_done",
                )

                # Encadeamento interno das tasks do grupo
                (
                    t_setup_topic
                    >> t_start_engine
                    >> t_start_producer
                    >> t_warmup
                    >> t_start_metrics
                    >> t_run_experiment
                    >> t_stop_metrics
                    >> t_stop_producer
                    >> t_stop_engine
                )

            # Encadeamento sequencial entre grupos (não paralelo):
            # garante que Flink e Spark nunca disputem recursos
            if last_task is not None:
                last_task >> tg

            last_task = tg

    # Task final: gera relatório consolidado após todos os cenários
    t_generate_report = PythonOperator(
        task_id="generate_final_report",
        python_callable=_generate_report,
        dag=dag,
        doc_md=(
            "Gera o relatório JSON + CSV + tabela de comparação "
            "Flink vs Spark após todos os cenários terem sido executados."
        ),
    )

    if last_task is not None:
        last_task >> t_generate_report
