"""Executor generico de etapas por carga: estado, log, tratamento de falha por data.

Cada carga (data) passa por: PENDENTE -> BRONZE_OK -> SILVER_OK -> DIMENSOES_OK -> SUCESSO.
Se uma data falha, so ela vai para ERRO_<etapa>; as demais seguem. No fim, a etapa falha (o job fica
vermelho) para o alerta disparar, mas o estado de cada data fica registrado na tabela de controle.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession

from .config import Settings
from .control import Carga, Execucao, atualizar_carga, listar_cargas

log = logging.getLogger("mkt_pipeline")


@dataclass
class Resultado:
    lidas: int | None = None
    gravadas: int | None = None
    rejeitadas: int | None = None
    mensagem: str | None = None
    campos: dict[str, Any] = field(default_factory=dict)  # colunas de ctl_carga_arquivo a atualizar


class EtapaComFalhas(RuntimeError):
    """Uma ou mais cargas falharam na etapa (detalhes em log_erro e ctl_carga_arquivo)."""


def sobrescrever_carga(df: DataFrame, tabela: str, condicao: Column) -> None:
    """Sobrescreve atomicamente so as linhas da carga (idempotencia: reprocessar nao duplica)."""
    df.writeTo(tabela).overwrite(condicao)


def processar_cargas(
    spark: SparkSession,
    cfg: Settings,
    *,
    etapa: str,
    camada: str,
    tabela: str | None,
    status_entrada: str,
    status_sucesso: str,
    status_erro: str,
    func: Callable[[Carga], Resultado],
) -> None:
    cargas = listar_cargas(spark, cfg, status_entrada)
    if not cargas:
        with Execucao(spark, cfg, etapa, camada, tabela) as ex:
            ex.registrar(mensagem="Sem cargas pendentes para esta etapa")
        return

    falhas: list[tuple[str, Exception]] = []
    for carga in cargas:
        try:
            with Execucao(spark, cfg, etapa, camada, tabela, carga.data_carga) as ex:
                res = func(carga)
                ex.registrar(res.lidas, res.gravadas, res.rejeitadas, res.mensagem)
            atualizar_carga(spark, cfg, carga.data_carga, status_sucesso, **res.campos)
        except Exception as exc:  # noqa: BLE001 - registrado, e re-levantado ao fim da etapa
            try:
                atualizar_carga(spark, cfg, carga.data_carga, status_erro, mensagem=str(exc)[:1000])
            except Exception as e2:  # noqa: BLE001
                log.error("Falha ao marcar carga %s como %s: %s", carga.data_carga, status_erro, e2)
            falhas.append((carga.data_carga.isoformat(), exc))

    if falhas:
        resumo = "; ".join(f"{d}: {type(e).__name__}: {e}" for d, e in falhas)
        raise EtapaComFalhas(f"Etapa {etapa} falhou em {len(falhas)}/{len(cargas)} carga(s): {resumo}")
