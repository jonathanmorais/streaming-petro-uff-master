"""
Operadores stateful customizados para o pipeline Flink do benchmark 3W.

O WellStreamOperator mantém uma janela deslizante das últimas N observações
por poço e calcula features simples (mean, std) para simular workload de
Digital Twin sem implementar detecção de anomalias real.
"""
import math
import time
from typing import Iterator

from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.datastream.timerservice import TimerService
from pyflink.common.typeinfo import Types

from flink_pipeline.config import (
    SENSOR_FIELDS,
    STATE_CLEANUP_TIMER_SECONDS,
    WINDOW_SIZE_SECONDS,
)


class WellStreamOperator(KeyedProcessFunction):
    """
    Operador stateful por poço (well_id) para o benchmark 3W.

    Mantém janela deslizante das últimas WINDOW_SIZE observações.
    Calcula features simples (mean, std) de variáveis de pressão e temperatura.
    Emite resultado com latência fim-a-fim calculada.

    A lógica é intencionalmente MINIMAL para não contaminar a medição de
    desempenho da infraestrutura com custo de processamento de ML.
    """

    # Número de observações na janela deslizante (1 Hz → 60 segundos)
    WINDOW_SIZE: int = WINDOW_SIZE_SECONDS

    # Variáveis usadas no cálculo de features (subset representativo)
    FEATURE_VARS: list[str] = ["P-PDG", "P-TPT", "T-JUS-CKP", "T-TPT", "QGL"]

    def open(self, runtime_context: RuntimeContext) -> None:
        """Inicializa estados gerenciados pelo Flink (sobrevivem a checkpoints)."""
        # Janela deslizante: lista de observações recentes
        self._window_state = runtime_context.get_list_state(
            ListStateDescriptor(
                "observation_window",
                Types.PICKLED_BYTE_ARRAY(),
            )
        )
        # Contador de eventos anômalos (class != 0)
        self._anomaly_counter = runtime_context.get_state(
            ValueStateDescriptor("anomaly_counter", Types.LONG())
        )
        # Último timestamp processado (para detectar inatividade)
        self._last_event_ts = runtime_context.get_state(
            ValueStateDescriptor("last_event_ts", Types.LONG())
        )

    def process_element(
        self,
        value: dict,
        ctx: "KeyedProcessFunction.Context",
    ) -> Iterator[dict]:
        """
        Processa cada observação do poço.

        Args:
            value: Dicionário com os campos da mensagem Kafka deserializada.
            ctx: Contexto do operador (acesso a timer service e estado).

        Yields:
            Dicionário com métricas de latência e features calculadas.
        """
        consumer_ts_ms = int(time.time() * 1000)
        producer_ts_ms = value.get("producer_ts_ms", consumer_ts_ms)
        e2e_latency_ms = consumer_ts_ms - producer_ts_ms

        # 1. Atualizar janela deslizante
        obs = {}
        for k in self.FEATURE_VARS:
            v = value.get(k)
            if v is not None:
                try:
                    obs[k] = float(v)
                except (TypeError, ValueError):
                    pass
        current_window = list(self._window_state.get() or [])
        current_window.append(obs)

        # Manter apenas as últimas WINDOW_SIZE observações
        if len(current_window) > self.WINDOW_SIZE:
            current_window = current_window[-self.WINDOW_SIZE:]

        self._window_state.update(current_window)

        # 2. Atualizar contador de anomalias
        event_class = value.get("class") or 0
        counter = self._anomaly_counter.value() or 0
        if event_class != 0:
            counter += 1
            self._anomaly_counter.update(counter)

        # 3. Calcular features simples sobre a janela
        features = {}
        for var in self.FEATURE_VARS:
            vals = [
                float(obs[var]) for obs in current_window
                if obs.get(var) is not None
            ]
            features[f"{var}_mean"] = self._safe_mean(vals)
            features[f"{var}_std"] = self._safe_std(vals)

        # 4. Registrar timer para limpeza de estado (30 min sem dados)
        timer_ts = ctx.timestamp() + STATE_CLEANUP_TIMER_SECONDS * 1000
        ctx.timer_service().register_event_time_timer(timer_ts)
        self._last_event_ts.update(ctx.timestamp())

        # 5. Emitir resultado
        yield {
            "well_id":          value.get("well_id"),
            "event_code":       value.get("event_code"),
            "class":            event_class,
            "state":            value.get("state"),
            "producer_ts_ms":   producer_ts_ms,
            "consumer_ts_ms":   consumer_ts_ms,
            "e2e_latency_ms":   e2e_latency_ms,
            "window_size":      len(current_window),
            "anomaly_count":    counter,
            **features,
        }

    def on_timer(
        self,
        timestamp: int,
        ctx: "KeyedProcessFunction.OnTimerContext",
    ) -> Iterator[dict]:
        """
        Limpa estado de poços inativos após STATE_CLEANUP_TIMER_SECONDS.
        """
        last_ts = self._last_event_ts.value() or 0
        if timestamp - last_ts >= STATE_CLEANUP_TIMER_SECONDS * 1000:
            self._window_state.clear()
            self._anomaly_counter.clear()
            self._last_event_ts.clear()
        return iter([])

    # ------------------------------------------------------------------
    # Utilitários estatísticos
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_mean(values: list[float]) -> float | None:
        """Calcula média ignorando valores ausentes."""
        if not values:
            return None
        return sum(values) / len(values)

    @staticmethod
    def _safe_std(values: list[float]) -> float | None:
        """Calcula desvio padrão amostral ignorando valores ausentes."""
        n = len(values)
        if n < 2:
            return None
        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / (n - 1)
        return math.sqrt(variance)
