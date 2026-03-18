"""
Injeção de falhas para o cenário de fault tolerance do benchmark 3W.

O módulo mata containers de workers via ``docker kill`` e monitora a
recuperação através do consumer lag do Kafka, calculando métricas como
tempo de recuperação e possível perda/duplicação de mensagens.
"""

from __future__ import annotations

import subprocess
import time
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from benchmark.metrics_collector import MetricsCollector
    from benchmark.scenarios import Scenario

log = structlog.get_logger()

# Prefixos de container por engine (conforme docker-compose.yml)
_CONTAINER_PREFIX: dict[str, str] = {
    "flink": "flink-taskmanager",
    "spark": "spark-worker",
}


class FaultInjector:
    """Injeta falhas de infraestrutura e monitora a recuperação do sistema.

    Parâmetros
    ----------
    engine:
        Engine de stream processing alvo: ``"flink"`` ou ``"spark"``.
    docker_compose_project:
        Nome do projeto docker-compose (variável ``COMPOSE_PROJECT_NAME``),
        usado para montar o nome completo do container.
        Ex: ``"benchmark3w"`` → container ``benchmark3w-flink-taskmanager-2-1``.
    """

    def __init__(self, engine: str, docker_compose_project: str) -> None:
        if engine not in _CONTAINER_PREFIX:
            raise ValueError(
                f"Engine inválido: '{engine}'. Use 'flink' ou 'spark'."
            )
        self.engine = engine
        self.project = docker_compose_project
        self._prefix = _CONTAINER_PREFIX[engine]

    # ------------------------------------------------------------------
    # Construção do nome do container
    # ------------------------------------------------------------------

    def _container_name(self, worker_index: int) -> str:
        """Retorna o nome completo do container Docker para o worker index.

        O formato segue a convenção do docker-compose v3:
        ``<project>-<service>-<index>-1``.

        Parâmetros
        ----------
        worker_index:
            Índice do worker (1-based), conforme configurado no cenário.

        Retorna
        -------
        str
            Nome do container (ex: ``"benchmark3w-flink-taskmanager-2-1"``).
        """
        return f"{self.project}-{self._prefix}-{worker_index}-1"

    # ------------------------------------------------------------------
    # Matar worker
    # ------------------------------------------------------------------

    def kill_worker(self, worker_index: int) -> float:
        """Mata o container do worker especificado via ``docker kill``.

        O sinal enviado é ``SIGKILL`` (parada abrupta), simulando uma falha
        catastrófica de nó — o cenário mais adverso para avaliação de
        fault tolerance.

        Parâmetros
        ----------
        worker_index:
            Índice do worker a ser morto (1-based).  Para Flink, mata o
            TaskManager; para Spark, mata o Worker.

        Retorna
        -------
        float
            Timestamp Unix em milissegundos (``T_fault``) do momento em que
            o sinal foi enviado ao container.

        Raises
        ------
        RuntimeError
            Se o comando ``docker kill`` retornar código de saída não zero.
        """
        container = self._container_name(worker_index)
        logger = log.bind(engine=self.engine, container=container, worker_index=worker_index)

        logger.info("injetando_falha_kill_worker")

        result = subprocess.run(
            ["docker", "kill", container],
            capture_output=True,
            text=True,
        )

        t_fault_ms = time.time() * 1000.0

        if result.returncode != 0:
            error_msg = result.stderr.strip() or result.stdout.strip()
            logger.error("docker_kill_falhou", stderr=error_msg, returncode=result.returncode)
            raise RuntimeError(
                f"docker kill '{container}' falhou (código {result.returncode}): {error_msg}"
            )

        logger.info(
            "worker_morto",
            t_fault_ms=t_fault_ms,
            container=container,
        )
        return t_fault_ms

    # ------------------------------------------------------------------
    # Monitorar recuperação
    # ------------------------------------------------------------------

    def wait_for_recovery(
        self,
        metrics_collector: MetricsCollector,
        group_id: str,
        topic: str,
        timeout_s: int = 300,
        poll_interval_s: int = 5,
    ) -> float | None:
        """Monitora o consumer lag até retornar a zero (recuperação completa).

        A recuperação é considerada completa quando o lag agregado de **todas**
        as partições do tópico monitorado cai a zero por **dois polls
        consecutivos** (para evitar falso positivo transitório).

        Parâmetros
        ----------
        metrics_collector:
            Instância de :class:`MetricsCollector` para coletar o lag.
        group_id:
            Identificador do consumer group monitorado.
        topic:
            Nome do tópico Kafka.
        timeout_s:
            Timeout máximo em segundos.  Se atingido, retorna ``None``.
        poll_interval_s:
            Intervalo entre cada verificação de lag em segundos.

        Retorna
        -------
        float | None
            Timestamp Unix em milissegundos (``T_recovery``) do momento em
            que o lag zerou, ou ``None`` se o timeout foi atingido.
        """
        logger = log.bind(group_id=group_id, topic=topic, timeout_s=timeout_s)
        logger.info("aguardando_recuperação")

        deadline = time.monotonic() + timeout_s
        consecutive_zero = 0

        while time.monotonic() < deadline:
            lag_by_partition = metrics_collector.collect_kafka_consumer_lag(
                group_id=group_id,
                topic=topic,
            )

            total_lag = sum(
                v for v in lag_by_partition.values() if v is not None
            )
            partition_count = len(lag_by_partition)

            logger.debug(
                "lag_atual",
                total_lag=total_lag,
                partitions=partition_count,
                detail=lag_by_partition,
            )

            if total_lag == 0 and partition_count > 0:
                consecutive_zero += 1
                if consecutive_zero >= 2:
                    t_recovery_ms = time.time() * 1000.0
                    logger.info("recuperação_confirmada", t_recovery_ms=t_recovery_ms)
                    return t_recovery_ms
            else:
                consecutive_zero = 0

            time.sleep(poll_interval_s)

        logger.warning("timeout_aguardando_recuperação", timeout_s=timeout_s)
        return None

    # ------------------------------------------------------------------
    # Cenário completo de fault
    # ------------------------------------------------------------------

    def run_fault_scenario(
        self,
        scenario: Scenario,
        metrics_collector: MetricsCollector,
        group_id: str | None = None,
        topic: str | None = None,
    ) -> dict[str, Any]:
        """Executa o cenário completo de injeção de falha e coleta métricas.

        Fluxo de execução:

        1. Aguarda o warm-up definido no cenário.
        2. Aguarda o período estável adicional (``fault["after_seconds"]``).
        3. Mata o worker alvo com :meth:`kill_worker`.
        4. Monitora a recuperação com :meth:`wait_for_recovery`.
        5. Calcula e retorna as métricas do cenário de fault.

        A verificação de mensagens perdidas/duplicadas usa a diferença entre
        offsets Kafka antes e após o kill — uma aproximação conservadora.
        Para auditoria exata de exactly-once, comparar os resultados do
        tópico ``3w-results`` com o tópico de entrada.

        Parâmetros
        ----------
        scenario:
            Cenário de benchmark com configuração de fault em
            ``scenario.fault``.
        metrics_collector:
            Instância de :class:`MetricsCollector` para monitoramento.
        group_id:
            Consumer group a monitorar.  Se ``None``, usa
            ``"flink-3w-consumer"`` para Flink e ``"spark-3w-consumer"``
            para Spark.
        topic:
            Tópico Kafka a monitorar.  Se ``None``, usa ``"3w-sensors"``.

        Retorna
        -------
        dict[str, Any]
            Dicionário com métricas do cenário:

            - ``t_fault_ms``: timestamp da falha (ms Unix).
            - ``t_recovery_ms``: timestamp da recuperação (ms Unix), ou ``None``.
            - ``recovery_time_s``: tempo de recuperação em segundos, ou ``None``.
            - ``messages_lost``: estimativa de mensagens perdidas (0 com exactly-once).
            - ``messages_duplicated``: estimativa de mensagens duplicadas.
            - ``lag_at_fault``: lag total no momento da falha.
            - ``recovered``: booleano indicando se a recuperação foi bem-sucedida.

        Raises
        ------
        ValueError
            Se ``scenario.fault`` for ``None`` ou não contiver ``"target"``.
        """
        import os

        if not scenario.fault:
            raise ValueError(
                f"Cenário '{scenario.name}' não tem configuração de fault."
            )

        fault_cfg = scenario.fault
        worker_target: int = fault_cfg.get("target", 1)
        after_seconds: int = fault_cfg.get("after_seconds", 120)

        effective_group = group_id or (
            "flink-3w-consumer" if self.engine == "flink" else "spark-3w-consumer"
        )
        effective_topic = topic or os.getenv("KAFKA_TOPIC", "3w-sensors")

        logger = log.bind(
            scenario=scenario.name,
            engine=self.engine,
            worker_target=worker_target,
            after_seconds=after_seconds,
        )

        # 1. Aguardar warm-up
        logger.info("aguardando_warmup", warmup_s=scenario.warmup_s)
        time.sleep(scenario.warmup_s)

        # 2. Aguardar período estável adicional
        logger.info("aguardando_estabilização", seconds=after_seconds)
        time.sleep(after_seconds)

        # Captura lag antes da falha para estimar mensagens perdidas
        lag_before = metrics_collector.collect_kafka_consumer_lag(
            group_id=effective_group,
            topic=effective_topic,
        )
        lag_at_fault = sum(v for v in lag_before.values() if v is not None)

        # 3. Matar worker
        t_fault_ms = self.kill_worker(worker_index=worker_target)

        # 4. Aguardar recuperação
        t_recovery_ms = self.wait_for_recovery(
            metrics_collector=metrics_collector,
            group_id=effective_group,
            topic=effective_topic,
            timeout_s=300,
        )

        # 5. Calcular métricas
        recovery_time_s: float | None = None
        if t_recovery_ms is not None:
            recovery_time_s = (t_recovery_ms - t_fault_ms) / 1000.0

        # Lag após recuperação — reprocessamento indica possíveis duplicatas
        lag_after = metrics_collector.collect_kafka_consumer_lag(
            group_id=effective_group,
            topic=effective_topic,
        )
        lag_at_recovery = sum(v for v in lag_after.values() if v is not None)

        # Estimativa conservadora: mensagens não processadas durante a falha
        messages_lost = max(0, lag_at_recovery - lag_at_fault)
        # Duplicatas são detectadas indiretamente (exige auditoria externa)
        messages_duplicated = 0  # exige comparação de output vs input topics

        result: dict[str, Any] = {
            "t_fault_ms": t_fault_ms,
            "t_recovery_ms": t_recovery_ms,
            "recovery_time_s": recovery_time_s,
            "messages_lost": messages_lost,
            "messages_duplicated": messages_duplicated,
            "lag_at_fault": lag_at_fault,
            "recovered": t_recovery_ms is not None,
            "worker_killed": worker_target,
            "engine": self.engine,
            "scenario": scenario.name,
        }

        logger.info("cenário_de_fault_concluído", **result)
        return result
