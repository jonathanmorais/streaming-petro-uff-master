"""
Schema e construção de mensagens Kafka para o dataset 3W.
Trata NaN/NA do pandas convertendo para None (JSON null).
"""
import math
import time
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Campos de sensores (27 variáveis do 3W v2.0.0)
# ---------------------------------------------------------------------------
SENSOR_FIELDS: list[str] = [
    "P-PDG",
    "P-TPT",
    "T-TPT",
    "P-MON-CKP",
    "T-JUS-CKP",
    "P-JUS-CKP",
    "P-MON-CKGL",
    "P-JUS-CKGL",
    "T-JUS-CKGL",
    "P-MON-SDV-P",
    "PT-P",
    "T-MON-CKP",
    "T-PDG",
    "QGL",
    "QBS",
]

# Descrição do schema completo da mensagem Kafka
MESSAGE_SCHEMA: dict[str, type] = {
    # Metadados do poço
    "well_id":            str,    # ex: "REAL_20180905204436"
    "source":             str,    # "REAL" | "SIMULATED" | "DRAWN"
    "event_code":         int,    # 0-8 (diretório pai no dataset)
    # Timestamps
    "timestamp":          str,    # ISO 8601: "2018-09-05T20:44:36Z"
    "timestamp_ms":       int,    # Unix timestamp em milissegundos (event-time)
    "producer_ts_ms":     int,    # Timestamp de publicação no Kafka (processing-time)
    # Variáveis de pressão [Pa]
    "P-PDG":              float,
    "P-TPT":              float,
    "P-MON-CKP":          float,
    "P-JUS-CKP":          float,
    "P-MON-CKGL":         float,
    "P-JUS-CKGL":         float,
    "P-MON-SDV-P":        float,
    "PT-P":               float,
    # Variáveis de temperatura [°C]
    "T-TPT":              float,
    "T-JUS-CKP":          float,
    "T-MON-CKP":          float,
    "T-PDG":              float,
    "T-JUS-CKGL":         float,
    # Variáveis de vazão [m³/s]
    "QGL":                float,
    "QBS":                float,
    # Labels
    "class":              int,    # 0-8 estado estacionário, 101-108 transiente
    "state":              int,    # estado operacional
    # Progresso dentro da instância
    "instance_progress":  float,  # 0.0-1.0
}


def _safe_float(value: Any) -> float | None:
    """Converte valor para float, retornando None em caso de NaN/NA/None."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    f = float(value)
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _safe_int(value: Any) -> int | None:
    """Converte valor para int, retornando None em caso de NaN/NA/None."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return int(value)


def build_message(
    well_id: str,
    source: str,
    event_code: int,
    timestamp: pd.Timestamp,
    row: pd.Series,
    instance_progress: float,
) -> dict:
    """
    Constrói mensagem JSON para publicação no Kafka.

    Args:
        well_id: Identificador do poço (ex: REAL_20180905204436).
        source: Origem da instância (REAL, SIMULATED ou DRAWN).
        event_code: Código do evento (0-8).
        timestamp: Timestamp original do sensor (DatetimeIndex).
        row: Linha do DataFrame com os valores dos sensores.
        instance_progress: Progresso dentro da instância (0.0-1.0).

    Returns:
        Dicionário pronto para serialização JSON.
    """
    # Converte timestamp para milissegundos Unix
    ts_ms = int(timestamp.timestamp() * 1000)
    producer_ts_ms = int(time.time() * 1000)

    msg: dict = {
        "well_id":           well_id,
        "source":            source,
        "event_code":        event_code,
        "timestamp":         timestamp.isoformat(),
        "timestamp_ms":      ts_ms,
        "producer_ts_ms":    producer_ts_ms,
        "instance_progress": round(instance_progress, 4),
    }

    # Campos de sensores (NaN → None)
    for field in SENSOR_FIELDS:
        msg[field] = _safe_float(row.get(field))

    # Labels
    msg["class"] = _safe_int(row.get("class"))
    msg["state"] = _safe_int(row.get("state"))

    return msg
