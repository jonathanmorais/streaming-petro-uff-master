"""
Definição de cenários do benchmark Flink vs Spark Structured Streaming.

Cada cenário encapsula os parâmetros necessários para reproduzir um
experimento controlado sobre o dataset 3W da Petrobras.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import structlog

log = structlog.get_logger()


@dataclass
class Scenario:
    """Representa um cenário de benchmark com todos os seus parâmetros.

    Atributos
    ----------
    name:
        Identificador único do cenário (ex: ``"baseline_1x"``).
    speed_factor:
        Multiplicador de velocidade do replay. ``1.0`` = tempo real,
        ``5.0`` = 5× mais rápido, etc.
    workers:
        Número de workers (TaskManagers no Flink / Executors no Spark) a
        escalar via docker-compose antes de iniciar o experimento.
    partitions:
        Número de partições do tópico Kafka ``3w-sensors`` neste cenário.
    fault:
        Dicionário de configuração de injeção de falha, ou ``None`` se o
        cenário não envolve fault tolerance.  Formato esperado::

            {
                "type": "kill_worker",   # único tipo suportado
                "target": 2,             # índice do worker a ser morto
                "after_seconds": 120,    # segundos após estabilização
            }

    duration_s:
        Duração total do experimento em segundos (incluindo warm-up).
    warmup_s:
        Duração do período de aquecimento em segundos.  Métricas coletadas
        durante este período são descartadas da análise.
    """

    name: str
    speed_factor: float
    workers: int
    partitions: int
    fault: dict[str, Any] | None = None
    duration_s: int = 600
    warmup_s: int = 180

    @property
    def collection_s(self) -> int:
        """Duração efetiva de coleta de métricas (após warm-up)."""
        return self.duration_s - self.warmup_s

    def __str__(self) -> str:
        parts = [
            f"name={self.name}",
            f"speed_factor={self.speed_factor}x",
            f"workers={self.workers}",
            f"partitions={self.partitions}",
            f"duration={self.duration_s}s",
            f"warmup={self.warmup_s}s",
        ]
        if self.fault:
            parts.append(f"fault={self.fault}")
        return "Scenario(" + ", ".join(parts) + ")"


# ---------------------------------------------------------------------------
# Matriz completa de experimentos (seção 9 do CLAUDE.MD)
# ---------------------------------------------------------------------------

SCENARIOS: list[Scenario] = [
    # -- Baseline (velocidade real do dataset) --------------------------------
    Scenario(
        name="baseline_1x",
        speed_factor=1.0,
        workers=2,
        partitions=4,
    ),

    # -- Escalabilidade de carga (stress test) --------------------------------
    Scenario(
        name="load_5x",
        speed_factor=5.0,
        workers=2,
        partitions=4,
    ),
    Scenario(
        name="load_10x",
        speed_factor=10.0,
        workers=2,
        partitions=4,
    ),

    # -- Escalabilidade horizontal de workers (carga 5×) ----------------------
    Scenario(
        name="scale_1w",
        speed_factor=5.0,
        workers=1,
        partitions=4,
    ),
    Scenario(
        name="scale_2w",
        speed_factor=5.0,
        workers=2,
        partitions=4,
    ),
    Scenario(
        name="scale_3w",
        speed_factor=5.0,
        workers=3,
        partitions=8,
    ),
    Scenario(
        name="scale_5w",
        speed_factor=5.0,
        workers=5,
        partitions=8,
    ),

    # -- Escalabilidade de partições Kafka (workers=3) ------------------------
    Scenario(
        name="partitions_4",
        speed_factor=5.0,
        workers=3,
        partitions=4,
    ),
    Scenario(
        name="partitions_8",
        speed_factor=5.0,
        workers=3,
        partitions=8,
    ),
    Scenario(
        name="partitions_16",
        speed_factor=5.0,
        workers=3,
        partitions=16,
    ),
    Scenario(
        name="partitions_32",
        speed_factor=5.0,
        workers=3,
        partitions=32,
    ),

    # -- Fault tolerance (injeção de falha no worker 2 após 2 min estável) ----
    Scenario(
        name="fault_worker",
        speed_factor=5.0,
        workers=3,
        partitions=8,
        fault={
            "type": "kill_worker",
            "target": 2,
            "after_seconds": 120,
        },
    ),
]

# Índice rápido por nome
SCENARIOS_BY_NAME: dict[str, Scenario] = {s.name: s for s in SCENARIOS}


# ---------------------------------------------------------------------------
# Administração do tópico Kafka
# ---------------------------------------------------------------------------

def setup_kafka_topic(
    bootstrap_servers: str,
    topic: str,
    partitions: int,
    replication_factor: int = 3,
    retention_ms: int = 604_800_000,
    max_message_bytes: int = 1_048_576,
) -> None:
    """Cria ou recria o tópico Kafka com o número de partições especificado.

    Se o tópico já existir, ele é **deletado** e recriado com os novos
    parâmetros.  Essa abordagem garante que cada cenário de benchmark inicia
    com um tópico limpo, sem offsets ou dados residuais.

    Parâmetros
    ----------
    bootstrap_servers:
        String de brokers Kafka separada por vírgula
        (ex: ``"kafka-1:9092,kafka-2:9092,kafka-3:9092"``).
    topic:
        Nome do tópico a criar/recriar.
    partitions:
        Número de partições desejado.
    replication_factor:
        Fator de replicação.  Para ambientes com 3 brokers, use ``3``.
    retention_ms:
        Tempo de retenção em milissegundos.  Padrão: 7 dias.
    max_message_bytes:
        Tamanho máximo de mensagem em bytes.  Padrão: 1 MiB.

    Raises
    ------
    RuntimeError
        Se a operação de criação falhar após aguardar a deleção do tópico.
    """
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

    logger = log.bind(topic=topic, partitions=partitions, replication_factor=replication_factor)

    brokers = [b.strip() for b in bootstrap_servers.split(",")]
    admin = KafkaAdminClient(bootstrap_servers=brokers, client_id="benchmark-admin")

    try:
        # Verifica se o tópico existe e deleta antes de recriar
        existing = admin.list_topics()
        if topic in existing:
            logger.info("tópico_existente_deletando")
            admin.delete_topics([topic])
            # Aguarda a deleção propagar pelo cluster
            _wait_topic_deletion(admin, topic, timeout_s=30)

        new_topic = NewTopic(
            name=topic,
            num_partitions=partitions,
            replication_factor=replication_factor,
            topic_configs={
                "retention.ms": str(retention_ms),
                "max.message.bytes": str(max_message_bytes),
            },
        )
        admin.create_topics([new_topic])
        logger.info("tópico_criado_com_sucesso")

    except TopicAlreadyExistsError:
        logger.warning("tópico_já_existia_após_recriação_ignorando")
    finally:
        admin.close()


def _wait_topic_deletion(admin: Any, topic: str, timeout_s: int = 30) -> None:
    """Aguarda a deleção do tópico propagar pelo cluster Kafka.

    Parâmetros
    ----------
    admin:
        Instância de ``KafkaAdminClient``.
    topic:
        Nome do tópico aguardado para deleção.
    timeout_s:
        Timeout máximo em segundos antes de levantar ``TimeoutError``.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if topic not in admin.list_topics():
            return
        time.sleep(1.0)
    raise TimeoutError(
        f"Tópico '{topic}' ainda existe após {timeout_s}s aguardando deleção."
    )


# ---------------------------------------------------------------------------
# Execução de cenário individual
# ---------------------------------------------------------------------------

def run_scenario(scenario: Scenario, engine: str) -> None:
    """Executa um único cenário de benchmark para o engine especificado.

    A lógica de orquestração completa (setup → producer → engine →
    coleta → relatório) é delegada ao DAG Airflow ou executada de forma
    simplificada aqui para testes manuais.

    Parâmetros
    ----------
    scenario:
        Instância do cenário a executar.
    engine:
        Engine a utilizar: ``"flink"`` ou ``"spark"``.

    Raises
    ------
    ValueError
        Se ``engine`` não for ``"flink"`` nem ``"spark"``.
    """
    if engine not in {"flink", "spark"}:
        raise ValueError(f"Engine inválido: '{engine}'. Use 'flink' ou 'spark'.")

    logger = log.bind(scenario=scenario.name, engine=engine)
    logger.info("iniciando_cenário", scenario=str(scenario))

    # Importação local para evitar dependências circulares em tempo de importação
    from benchmark.metrics_collector import MetricsCollector
    from benchmark.fault_injector import FaultInjector
    import os
    import threading

    prometheus_url = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "3w-sensors")
    pushgateway_url = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
    results_path = os.getenv("RESULTS_PATH", "./results")
    project = os.getenv("COMPOSE_PROJECT_NAME", "benchmark3w")

    # (Re)cria tópico com o número correto de partições para este cenário
    setup_kafka_topic(
        bootstrap_servers=kafka_brokers,
        topic=topic,
        partitions=scenario.partitions,
    )

    collector = MetricsCollector(
        prometheus_url=prometheus_url,
        kafka_brokers=kafka_brokers,
        flink_jm_host=os.getenv("FLINK_JM_HOST", "localhost"),
        flink_jm_port=int(os.getenv("FLINK_JM_PORT", "8081")),
        pushgateway_url=pushgateway_url,
    )

    output_path = (
        f"{results_path}/raw/{scenario.name}/{engine}/metrics.jsonl"
    )
    import pathlib
    pathlib.Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    stop_event = threading.Event()

    logger.info("aguardando_warmup", warmup_s=scenario.warmup_s)
    time.sleep(scenario.warmup_s)

    # Inicia coleta de métricas em thread separada
    collection_thread = threading.Thread(
        target=collector.run_collection_loop,
        args=(10, output_path, stop_event),
        daemon=True,
    )
    collection_thread.start()

    if scenario.fault:
        injector = FaultInjector(engine=engine, docker_compose_project=project)
        fault_metrics = injector.run_fault_scenario(
            scenario=scenario,
            metrics_collector=collector,
        )
        logger.info("métricas_de_falha", **fault_metrics)
    else:
        logger.info("coletando_métricas", duration_s=scenario.collection_s)
        time.sleep(scenario.collection_s)

    stop_event.set()
    collection_thread.join(timeout=15)
    logger.info("cenário_concluído", output=output_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    """Constrói o parser de argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description="Gerenciamento de cenários do benchmark 3W Flink vs Spark.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Criar/recriar tópico com 16 partições
  python -m benchmark.scenarios --setup-topic --partitions 16

  # Listar todos os cenários disponíveis
  python -m benchmark.scenarios --list-scenarios

  # Executar cenário de carga com Flink
  python -m benchmark.scenarios --run-scenario load_5x --engine flink

  # Executar cenário de fault tolerance com Spark
  python -m benchmark.scenarios --run-scenario fault_worker --engine spark
""",
    )

    parser.add_argument(
        "--setup-topic",
        action="store_true",
        help="Cria ou recria o tópico Kafka '3w-sensors'.",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=8,
        metavar="N",
        help="Número de partições do tópico (padrão: 8). Usado com --setup-topic.",
    )
    parser.add_argument(
        "--list-scenarios",
        action="store_true",
        help="Lista todos os cenários disponíveis e seus parâmetros.",
    )
    parser.add_argument(
        "--run-scenario",
        metavar="NAME",
        help="Nome do cenário a executar (ex: baseline_1x, load_5x, fault_worker).",
    )
    parser.add_argument(
        "--engine",
        choices=["flink", "spark"],
        default="flink",
        help="Engine de stream processing a usar (padrão: flink).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        metavar="BROKERS",
        help=(
            "Bootstrap servers Kafka (padrão: usa env KAFKA_BROKERS ou "
            "'localhost:9092')."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada principal da CLI de cenários."""
    import os

    parser = _build_parser()
    args = parser.parse_args(argv)

    bootstrap_servers = (
        args.bootstrap_servers
        or os.getenv("KAFKA_BROKERS", "localhost:9092")
    )
    topic = os.getenv("KAFKA_TOPIC", "3w-sensors")

    if args.setup_topic:
        print(
            f"Criando/recriando tópico '{topic}' com {args.partitions} "
            f"partições em {bootstrap_servers} ..."
        )
        setup_kafka_topic(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            partitions=args.partitions,
        )
        print("Tópico configurado com sucesso.")
        return 0

    if args.list_scenarios:
        print(f"{'CENÁRIO':<20} {'SPEED':<8} {'WORKERS':<9} {'PARTIÇÕES':<11} {'DURAÇÃO':<9} {'FAULT'}")
        print("-" * 75)
        for s in SCENARIOS:
            fault_str = str(s.fault) if s.fault else "-"
            print(
                f"{s.name:<20} {s.speed_factor:<8.1f} {s.workers:<9} "
                f"{s.partitions:<11} {s.duration_s}s{'':<4} {fault_str}"
            )
        return 0

    if args.run_scenario:
        scenario = SCENARIOS_BY_NAME.get(args.run_scenario)
        if scenario is None:
            names = ", ".join(SCENARIOS_BY_NAME.keys())
            print(
                f"Erro: cenário '{args.run_scenario}' não encontrado. "
                f"Disponíveis: {names}",
                file=sys.stderr,
            )
            return 1
        run_scenario(scenario=scenario, engine=args.engine)
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
