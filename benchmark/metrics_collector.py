"""
Coleta de métricas do benchmark 3W: Prometheus, Kafka consumer lag,
Flink REST API e Spark UI REST API.

As métricas são salvas em arquivos JSONL (uma linha JSON por snapshot)
para análise posterior pelo módulo ``benchmark.report``.
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import numpy as np
import requests
import structlog
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import KafkaAdminClient as AdminClient

log = structlog.get_logger()

# Métricas Prometheus coletadas por padrão em cada snapshot
DEFAULT_PROMETHEUS_METRICS: list[str] = [
    # Flink
    "flink_taskmanager_job_task_numRecordsInPerSecond",
    "flink_taskmanager_job_task_numRecordsOutPerSecond",
    "flink_jobmanager_job_numRestarts",
    "flink_jobmanager_job_lastCheckpointDuration",
    "flink_jobmanager_job_lastCheckpointSize",
    # Flink latência (quantis emitidos pelo Flink internamente)
    'flink_taskmanager_job_latency_source_id_operator_id_taskmanager_id_quantile',
    # Spark
    "spark_streaming_inputRowsPerSecond",
    "spark_streaming_processedRowsPerSecond",
    "spark_streaming_batchDuration",
    "spark_streaming_triggerExecutionLatency",
    # Kafka exporter
    "kafka_consumergroup_lag",
    # Producer
    "3w_producer_messages_total",
    "3w_producer_messages_per_second",
]


class MetricsCollector:
    """Coleta métricas de todas as fontes do benchmark e persiste em JSONL.

    Parâmetros
    ----------
    prometheus_url:
        URL base do Prometheus (ex: ``"http://localhost:9090"``).
    kafka_brokers:
        String de bootstrap servers Kafka separada por vírgula.
    flink_jm_host:
        Hostname do Flink JobManager (ex: ``"localhost"``).
    flink_jm_port:
        Porta REST do Flink JobManager (padrão: ``8081``).
    pushgateway_url:
        URL do Prometheus Pushgateway para métricas do producer.
    spark_ui_port:
        Porta da Spark UI REST API (padrão: ``4040``).
    request_timeout_s:
        Timeout para requisições HTTP em segundos (padrão: ``10``).
    """

    def __init__(
        self,
        prometheus_url: str,
        kafka_brokers: str,
        flink_jm_host: str,
        flink_jm_port: int,
        pushgateway_url: str,
        spark_ui_host: str = "localhost",
        spark_ui_port: int = 4040,
        request_timeout_s: int = 10,
    ) -> None:
        self.prometheus_url = prometheus_url.rstrip("/")
        self.kafka_brokers = [b.strip() for b in kafka_brokers.split(",")]
        self.flink_base_url = f"http://{flink_jm_host}:{flink_jm_port}"
        self.pushgateway_url = pushgateway_url.rstrip("/")
        self.spark_ui_base_url = f"http://{spark_ui_host}:{spark_ui_port}"
        self.request_timeout_s = request_timeout_s
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    # ------------------------------------------------------------------
    # Coleta via Prometheus HTTP API
    # ------------------------------------------------------------------

    def collect_prometheus_snapshot(self, metrics: list[str] | None = None) -> dict[str, Any]:
        """Coleta snapshot de métricas do Prometheus via HTTP API (/api/v1/query).

        Para cada nome de métrica da lista, executa uma query instantânea no
        Prometheus e retorna o valor escalar (ou a soma de todos os time series
        com aquele nome, quando houver múltiplos).

        Parâmetros
        ----------
        metrics:
            Lista de nomes de métricas Prometheus a consultar.  Se ``None``,
            usa ``DEFAULT_PROMETHEUS_METRICS``.

        Retorna
        -------
        dict[str, Any]
            Dicionário ``{nome_metrica: valor_float}``.  Métricas ausentes ou
            que causaram erro ficam com valor ``None``.
        """
        if metrics is None:
            metrics = DEFAULT_PROMETHEUS_METRICS

        result: dict[str, Any] = {}
        endpoint = f"{self.prometheus_url}/api/v1/query"

        for metric_name in metrics:
            try:
                resp = self._session.get(
                    endpoint,
                    params={"query": metric_name},
                    timeout=self.request_timeout_s,
                )
                resp.raise_for_status()
                data = resp.json()

                vector_results = data.get("data", {}).get("result", [])
                if not vector_results:
                    result[metric_name] = None
                    continue

                # Agrega múltiplos time series somando os valores
                total = sum(
                    float(r["value"][1])
                    for r in vector_results
                    if r.get("value")
                )
                result[metric_name] = total

            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "prometheus_query_falhou",
                    metric=metric_name,
                    error=str(exc),
                )
                result[metric_name] = None

        return result

    # ------------------------------------------------------------------
    # Consumer lag via kafka-python
    # ------------------------------------------------------------------

    def collect_kafka_consumer_lag(
        self, group_id: str, topic: str
    ) -> dict[int, int | None]:
        """Coleta consumer lag por partição via kafka-python AdminClient.

        O lag é calculado como ``(latest_offset - committed_offset)`` para cada
        partição do tópico monitorado.

        Parâmetros
        ----------
        group_id:
            Identificador do consumer group (ex: ``"flink-3w-consumer"``).
        topic:
            Nome do tópico Kafka.

        Retorna
        -------
        dict[int, int | None]
            Dicionário ``{partition_id: lag}``.  ``None`` indica que não foi
            possível obter o offset para aquela partição.
        """
        from kafka import KafkaConsumer, TopicPartition
        from kafka.admin import KafkaAdminClient

        lag_by_partition: dict[int, int | None] = {}

        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.kafka_brokers,
                client_id="metrics-collector-admin",
            )
            # Offsets cometidos pelo consumer group
            offsets_response = admin.list_consumer_group_offsets(group_id)
            admin.close()

            # Consumer temporário para buscar latest offsets
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_brokers,
                group_id=None,  # não registra group
                enable_auto_commit=False,
            )

            # Filtra apenas partições do tópico alvo
            topic_partitions = [
                tp for tp in offsets_response.keys() if tp.topic == topic
            ]

            # Latest offsets (end offsets)
            end_offsets = consumer.end_offsets(topic_partitions)
            consumer.close()

            for tp in topic_partitions:
                committed = offsets_response.get(tp)
                latest = end_offsets.get(tp)
                if committed is not None and latest is not None:
                    lag_by_partition[tp.partition] = max(0, latest - committed.offset)
                else:
                    lag_by_partition[tp.partition] = None

        except Exception as exc:  # noqa: BLE001
            log.warning(
                "coleta_lag_falhou",
                group_id=group_id,
                topic=topic,
                error=str(exc),
            )

        return lag_by_partition

    # ------------------------------------------------------------------
    # Flink REST API
    # ------------------------------------------------------------------

    def collect_flink_metrics(self) -> dict[str, Any]:
        """Coleta métricas da Flink REST API (/jobs, /jobs/{id}/metrics).

        Consulta a lista de jobs ativos, seleciona o mais recente e coleta
        métricas de latência (quantis), throughput, checkpoints e número de
        reinicializações.

        Retorna
        -------
        dict[str, Any]
            Dicionário com campos:

            - ``job_id``: ID do job Flink ativo.
            - ``status``: estado do job (``"RUNNING"``, etc.).
            - ``records_in_per_second``: throughput de entrada.
            - ``records_out_per_second``: throughput de saída.
            - ``latency_p50``, ``latency_p95``, ``latency_p99``: quantis de latência (ms).
            - ``num_restarts``: contagem de reinicializações (indicador de falha).
            - ``last_checkpoint_duration_ms``: duração do último checkpoint (ms).
            - ``last_checkpoint_size_bytes``: tamanho do último checkpoint (bytes).
        """
        result: dict[str, Any] = {
            "job_id": None,
            "status": None,
            "records_in_per_second": None,
            "records_out_per_second": None,
            "latency_p50": None,
            "latency_p95": None,
            "latency_p99": None,
            "num_restarts": None,
            "last_checkpoint_duration_ms": None,
            "last_checkpoint_size_bytes": None,
        }

        try:
            # Lista jobs
            jobs_resp = self._session.get(
                f"{self.flink_base_url}/jobs",
                timeout=self.request_timeout_s,
            )
            jobs_resp.raise_for_status()
            jobs = jobs_resp.json().get("jobs", [])

            running = [j for j in jobs if j.get("status") == "RUNNING"]
            if not running:
                log.debug("flink_sem_jobs_running")
                return result

            # Usa o primeiro job RUNNING encontrado
            job_id = running[0]["id"]
            result["job_id"] = job_id
            result["status"] = "RUNNING"

            # Detalhes do job
            detail_resp = self._session.get(
                f"{self.flink_base_url}/jobs/{job_id}",
                timeout=self.request_timeout_s,
            )
            detail_resp.raise_for_status()
            detail = detail_resp.json()

            result["num_restarts"] = detail.get("duration", {})  # versão simplificada

            # Métricas do job
            metrics_resp = self._session.get(
                f"{self.flink_base_url}/jobs/{job_id}/metrics",
                params={
                    "get": (
                        "numRecordsInPerSecond,"
                        "numRecordsOutPerSecond,"
                        "numRestarts,"
                        "lastCheckpointDuration,"
                        "lastCheckpointSize,"
                        "latency.source_id.*.quantile.0.5,"
                        "latency.source_id.*.quantile.0.95,"
                        "latency.source_id.*.quantile.0.99"
                    )
                },
                timeout=self.request_timeout_s,
            )
            metrics_resp.raise_for_status()
            raw_metrics: list[dict] = metrics_resp.json()

            # Mapeia nome → valor
            metrics_map: dict[str, float] = {}
            for m in raw_metrics:
                try:
                    metrics_map[m["id"]] = float(m.get("value", 0))
                except (ValueError, TypeError):
                    pass

            result["records_in_per_second"] = metrics_map.get("numRecordsInPerSecond")
            result["records_out_per_second"] = metrics_map.get("numRecordsOutPerSecond")
            result["num_restarts"] = metrics_map.get("numRestarts")
            result["last_checkpoint_duration_ms"] = metrics_map.get("lastCheckpointDuration")
            result["last_checkpoint_size_bytes"] = metrics_map.get("lastCheckpointSize")

            # Quantis de latência (chaves variam com IDs de operador)
            p50 = [v for k, v in metrics_map.items() if "0.5" in k]
            p95 = [v for k, v in metrics_map.items() if "0.95" in k]
            p99 = [v for k, v in metrics_map.items() if "0.99" in k]
            result["latency_p50"] = float(np.mean(p50)) if p50 else None
            result["latency_p95"] = float(np.mean(p95)) if p95 else None
            result["latency_p99"] = float(np.mean(p99)) if p99 else None

        except Exception as exc:  # noqa: BLE001
            log.warning("flink_metrics_falhou", error=str(exc))

        return result

    # ------------------------------------------------------------------
    # Spark UI REST API
    # ------------------------------------------------------------------

    def collect_spark_metrics(self) -> dict[str, Any]:
        """Coleta métricas da Spark UI REST API (/api/v1/applications).

        Consulta a lista de aplicações ativas na Spark UI e extrai
        estatísticas de streaming (micro-batch) da aplicação mais recente.

        Retorna
        -------
        dict[str, Any]
            Dicionário com campos:

            - ``app_id``, ``app_name``: identificação da aplicação.
            - ``input_rows_per_second``: taxa de ingestão de linhas.
            - ``processed_rows_per_second``: taxa de processamento.
            - ``batch_duration_ms``: duração média de micro-batch (ms).
            - ``trigger_execution_latency_ms``: latência de execução do trigger (ms).
            - ``num_active_batches``: número de micro-batches em execução.
            - ``num_input_rows``: total de linhas processadas.
        """
        result: dict[str, Any] = {
            "app_id": None,
            "app_name": None,
            "input_rows_per_second": None,
            "processed_rows_per_second": None,
            "batch_duration_ms": None,
            "trigger_execution_latency_ms": None,
            "num_active_batches": None,
            "num_input_rows": None,
        }

        try:
            apps_resp = self._session.get(
                f"{self.spark_ui_base_url}/api/v1/applications",
                timeout=self.request_timeout_s,
            )
            apps_resp.raise_for_status()
            apps = apps_resp.json()

            if not apps:
                log.debug("spark_sem_aplicações_ativas")
                return result

            app = apps[0]
            app_id = app["id"]
            result["app_id"] = app_id
            result["app_name"] = app.get("name")

            # Consulta streaming statistics
            streams_resp = self._session.get(
                f"{self.spark_ui_base_url}/api/v1/applications/{app_id}/streaming/statistics",
                timeout=self.request_timeout_s,
            )
            streams_resp.raise_for_status()
            stats = streams_resp.json()

            result["input_rows_per_second"] = stats.get("avgInputRate")
            result["processed_rows_per_second"] = stats.get("avgProcessingRate")
            result["batch_duration_ms"] = stats.get("avgBatchDuration")
            result["trigger_execution_latency_ms"] = stats.get("avgSchedulingDelay")
            result["num_active_batches"] = stats.get("numActiveBatches")
            result["num_input_rows"] = stats.get("totalInputRows")

        except Exception as exc:  # noqa: BLE001
            log.warning("spark_metrics_falhou", error=str(exc))

        return result

    # ------------------------------------------------------------------
    # Loop principal de coleta
    # ------------------------------------------------------------------

    def run_collection_loop(
        self,
        interval_s: int,
        output_path: str,
        stop_event: threading.Event,
        group_id: str | None = None,
        topic: str | None = None,
    ) -> None:
        """Loop principal de coleta que salva snapshots em arquivo JSONL.

        Executa a cada ``interval_s`` segundos, coletando métricas de todas
        as fontes configuradas e persistindo o snapshot como uma linha JSON
        no arquivo ``output_path``.  O loop é encerrado quando ``stop_event``
        é sinalizado.

        Parâmetros
        ----------
        interval_s:
            Intervalo entre coletas em segundos.
        output_path:
            Caminho do arquivo JSONL de saída.  Será criado se não existir.
        stop_event:
            Evento de threading que sinaliza a parada do loop.
        group_id:
            Consumer group ID para coleta de lag.  Se ``None``, usa variável
            de ambiente ``KAFKA_GROUP_ID`` ou ``"flink-3w-consumer"``.
        topic:
            Tópico Kafka para coleta de lag.  Se ``None``, usa ``KAFKA_TOPIC``
            ou ``"3w-sensors"``.
        """
        effective_group = group_id or os.getenv("KAFKA_GROUP_ID", "flink-3w-consumer")
        effective_topic = topic or os.getenv("KAFKA_TOPIC", "3w-sensors")

        import pathlib
        pathlib.Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        logger = log.bind(output=output_path, interval_s=interval_s)
        logger.info("coleta_iniciada")

        iteration = 0
        with open(output_path, "a", encoding="utf-8") as fh:
            while not stop_event.is_set():
                ts_start = time.monotonic()
                collected_at = datetime.now(tz=timezone.utc).isoformat()

                snapshot: dict[str, Any] = {
                    "collected_at": collected_at,
                    "iteration": iteration,
                    "prometheus": self.collect_prometheus_snapshot(),
                    "consumer_lag": self.collect_kafka_consumer_lag(
                        group_id=effective_group,
                        topic=effective_topic,
                    ),
                    "flink": self.collect_flink_metrics(),
                    "spark": self.collect_spark_metrics(),
                }

                fh.write(json.dumps(snapshot, default=str) + "\n")
                fh.flush()

                logger.debug("snapshot_salvo", iteration=iteration)
                iteration += 1

                # Aguarda o próximo intervalo, verificando stop a cada 0.5s
                elapsed = time.monotonic() - ts_start
                remaining = max(0.0, interval_s - elapsed)
                stop_event.wait(timeout=remaining)

        logger.info("coleta_encerrada", total_snapshots=iteration)

    # ------------------------------------------------------------------
    # Estatísticas de latência
    # ------------------------------------------------------------------

    def compute_latency_percentiles(self, samples: list[float]) -> dict[str, float]:
        """Calcula p50, p95 e p99 de uma lista de latências.

        Parâmetros
        ----------
        samples:
            Lista de valores de latência em milissegundos.  Valores ``NaN``
            e ``None`` são filtrados antes do cálculo.

        Retorna
        -------
        dict[str, float]
            Dicionário com chaves ``"p50"``, ``"p95"``, ``"p99"`` e ``"count"``.
            Se não houver amostras válidas, todos os valores serão ``None``.
        """
        valid = [s for s in samples if s is not None and not (isinstance(s, float) and np.isnan(s))]

        if not valid:
            return {"p50": None, "p95": None, "p99": None, "count": 0}

        arr = np.array(valid, dtype=np.float64)
        return {
            "p50": float(np.percentile(arr, 50)),
            "p95": float(np.percentile(arr, 95)),
            "p99": float(np.percentile(arr, 99)),
            "count": len(valid),
        }

    # ------------------------------------------------------------------
    # Utilitários
    # ------------------------------------------------------------------

    def push_gauge(self, metric_name: str, value: float, labels: dict[str, str] | None = None) -> None:
        """Envia uma métrica gauge ao Prometheus Pushgateway.

        Parâmetros
        ----------
        metric_name:
            Nome da métrica no formato Prometheus (sem espaços).
        value:
            Valor numérico a publicar.
        labels:
            Labels adicionais (ex: ``{"engine": "flink", "scenario": "load_5x"}``).
        """
        label_str = ""
        if labels:
            label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())

        payload = f"# TYPE {metric_name} gauge\n{metric_name}{{{label_str}}} {value}\n"

        job = labels.get("job", "benchmark3w") if labels else "benchmark3w"
        url = f"{self.pushgateway_url}/metrics/job/{job}"

        try:
            resp = self._session.post(
                url,
                data=payload,
                headers={"Content-Type": "text/plain"},
                timeout=self.request_timeout_s,
            )
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            log.warning("pushgateway_push_falhou", metric=metric_name, error=str(exc))
