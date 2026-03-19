"""
Lógica de replay temporal do dataset 3W para o Kafka.

Cada arquivo Parquet é reproduzido linha por linha preservando os timestamps
originais. O sleep entre eventos é ajustado pelo speed_factor.
"""
import pathlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator

import pandas as pd
import pyarrow.parquet as pq
import structlog

from producer.schema import build_message

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Replay de um único poço
# ---------------------------------------------------------------------------
class WellReplayer:
    """
    Reproduz um arquivo Parquet do 3W como stream para o Kafka.

    O replay preserva os deltas de tempo originais entre observações,
    escalados pelo speed_factor. Cada instância roda em uma thread separada.
    """

    def __init__(
        self,
        parquet_path: pathlib.Path,
        producer,  # KafkaProducer
        topic: str,
        speed_factor: float = 1.0,
        loop: bool = False,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.parquet_path = parquet_path
        self.producer = producer
        self.topic = topic
        self.speed_factor = max(speed_factor, 0.001)
        self.loop = loop
        self.stop_event = stop_event or threading.Event()

        # Inferir metadados a partir do nome do arquivo
        stem = parquet_path.stem                          # ex: REAL_20180905204436
        parts = stem.split("_", 1)
        self.source = parts[0]                            # REAL | SIMULATED | DRAWN
        self.well_id = stem
        # event_code vem do diretório pai
        try:
            self.event_code = int(parquet_path.parent.name)
        except ValueError:
            self.event_code = -1

        self._messages_sent: int = 0
        self._errors: int = 0

    # ------------------------------------------------------------------
    def _iter_rows(self) -> Iterator[tuple[pd.Timestamp, pd.Series, float]]:
        """
        Itera linhas do Parquet em batches de 500 linhas para limitar uso de memória.
        Cada thread só mantém um batch em RAM em vez do arquivo inteiro.
        """
        pf = pq.ParquetFile(self.parquet_path)
        total = pf.metadata.num_rows
        idx = 0
        for batch in pf.iter_batches(batch_size=500):
            df_batch = batch.to_pandas()
            # Garantir DatetimeIndex no timestamp
            if "timestamp" in df_batch.columns:
                df_batch = df_batch.set_index("timestamp")
            if not isinstance(df_batch.index, pd.DatetimeIndex):
                df_batch.index = pd.to_datetime(df_batch.index)
            for ts, row in df_batch.iterrows():
                yield ts, row, (idx / total if total > 0 else 0.0)
                idx += 1

    def run(self) -> None:
        """
        Loop principal de replay.
        Lê o Parquet em batches e publica cada linha no Kafka com sleep
        proporcional ao delta de tempo entre observações dividido pelo speed_factor.
        """
        log = logger.bind(well_id=self.well_id, event_code=self.event_code)
        log.info("iniciando_replay", parquet=str(self.parquet_path))

        iteration = 0
        while not self.stop_event.is_set():
            iteration += 1
            prev_ts: pd.Timestamp | None = None

            try:
                for ts, row, progress in self._iter_rows():
                    if self.stop_event.is_set():
                        break

                    # Calcular sleep baseado no delta de tempo original
                    if prev_ts is not None:
                        delta_s = (ts - prev_ts).total_seconds()
                        sleep_s = delta_s / self.speed_factor
                        if sleep_s > 0:
                            end_time = time.monotonic() + sleep_s
                            while time.monotonic() < end_time and not self.stop_event.is_set():
                                time.sleep(min(0.05, end_time - time.monotonic()))
                    prev_ts = ts

                    if self.stop_event.is_set():
                        break

                    try:
                        msg = build_message(
                            well_id=self.well_id,
                            source=self.source,
                            event_code=self.event_code,
                            timestamp=ts,
                            row=row,
                            instance_progress=progress,
                        )
                        self.producer.send(
                            self.topic,
                            key=self.well_id.encode("utf-8"),
                            value=msg,
                        )
                        self._messages_sent += 1
                    except Exception as exc:
                        self._errors += 1
                        log.warning("erro_ao_publicar", error=str(exc))

            except Exception as exc:
                log.error("erro_ao_ler_parquet", error=str(exc))
                return

            log.debug("iteracao_concluida", iteration=iteration, messages=self._messages_sent)

            if not self.loop:
                break

        log.info("replay_encerrado", total_messages=self._messages_sent, errors=self._errors)

    @property
    def messages_sent(self) -> int:
        return self._messages_sent

    @property
    def errors(self) -> int:
        return self._errors


# ---------------------------------------------------------------------------
# Orquestrador — múltiplos poços em paralelo
# ---------------------------------------------------------------------------
class ReplayOrchestrator:
    """
    Orquestra o replay de múltiplas instâncias do dataset 3W em paralelo.

    Cada poço (arquivo Parquet) roda em uma thread separada, até o limite
    de max_concurrent_wells. Quando loop=True, instâncias que terminam são
    reiniciadas automaticamente.
    """

    def __init__(
        self,
        dataset_path: str,
        producer,  # KafkaProducer
        config: dict,
    ) -> None:
        self.dataset_path = pathlib.Path(dataset_path)
        self.producer = producer
        self.topic: str = config.get("topic", "3w-sensors")
        self.speed_factor: float = float(config.get("speed_factor", 1.0))
        self.event_codes: list[int] = config.get("event_codes", list(range(9)))
        self.sources: list[str] = config.get("sources", ["REAL", "SIMULATED"])
        self.max_concurrent_wells: int = int(config.get("max_concurrent_wells", 42))
        self.loop: bool = bool(config.get("loop", True))

        self._stop_event = threading.Event()
        self._instances: list[pathlib.Path] = []
        self._replayers: list[WellReplayer] = []

    def discover_instances(self) -> list[pathlib.Path]:
        """
        Descobre todos os arquivos Parquet que correspondem aos filtros
        de event_codes e sources configurados.
        """
        found: list[pathlib.Path] = []
        for code in self.event_codes:
            event_dir = self.dataset_path / str(code)
            if not event_dir.exists():
                logger.warning("diretorio_nao_encontrado", path=str(event_dir))
                continue
            for pf in sorted(event_dir.glob("*.parquet")):
                stem_source = pf.stem.split("_")[0].upper()
                if stem_source in [s.upper() for s in self.sources]:
                    found.append(pf)

        logger.info("instancias_descobertas", total=len(found))
        self._instances = found
        return found

    def run(self) -> None:
        """
        Inicia o replay de todas as instâncias com ThreadPoolExecutor.
        Limita a max_concurrent_wells threads simultâneas.
        """
        if not self._instances:
            self.discover_instances()

        if not self._instances:
            logger.error("nenhuma_instancia_encontrada")
            return

        selected = self._instances[: self.max_concurrent_wells]
        logger.info("iniciando_replay_orquestrado", wells=len(selected))

        self._replayers = [
            WellReplayer(
                parquet_path=path,
                producer=self.producer,
                topic=self.topic,
                speed_factor=self.speed_factor,
                loop=self.loop,
                stop_event=self._stop_event,
            )
            for path in selected
        ]

        with ThreadPoolExecutor(
            max_workers=len(self._replayers),
            thread_name_prefix="well-replay",
        ) as executor:
            futures = {executor.submit(r.run): r for r in self._replayers}
            try:
                for future in as_completed(futures):
                    replayer = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        logger.error(
                            "erro_na_thread",
                            well_id=replayer.well_id,
                            error=str(exc),
                        )
            except KeyboardInterrupt:
                logger.info("interrupcao_recebida_no_orquestrador")
                self.stop()

    def stop(self) -> None:
        """Sinaliza todas as threads para parar."""
        logger.info("parando_orquestrador")
        self._stop_event.set()

    @property
    def total_messages_sent(self) -> int:
        return sum(r.messages_sent for r in self._replayers)
