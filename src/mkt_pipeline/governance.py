"""Governanca no Unity Catalog: comentarios, tags, constraints informativas e dicionario de dados."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import ddl
from .config import Settings
from .control import agora, registrar_erro
from .metadata import TABELAS, classificar

log = logging.getLogger("mkt_pipeline")


def _q(texto: str) -> str:
    return texto.replace("\\", "\\\\").replace("'", "''")


def _tentar(spark: SparkSession, cfg: Settings, sql: str, contexto: str) -> bool:
    """Governanca e best-effort: uma tag/constraint nao suportada nao pode derrubar a carga de dados."""
    try:
        spark.sql(sql)
        return True
    except Exception as exc:  # noqa: BLE001
        registrar_erro(
            spark,
            cfg,
            "governanca",
            "GOVERNANCA_NAO_APLICADA",
            f"{contexto}: {str(exc)[:500]}",
            severidade="AVISO",
        )
        return False


def aplicar_comentarios_e_tags(spark: SparkSession, cfg: Settings) -> tuple[int, int]:
    aplicados = falhas = 0
    for tab in TABELAS:
        nome = cfg.tabela(tab.camada, tab.nome)
        comandos = [
            (f"COMMENT ON TABLE {nome} IS '{_q(tab.descricao)}'", f"comentario {nome}"),
            (
                f"ALTER TABLE {nome} SET TAGS ('camada' = '{tab.camada}', 'dominio' = 'marketing', "
                f"'ambiente' = '{cfg.ambiente}')",
                f"tags {nome}",
            ),
        ]
        existentes = {c.name for c in spark.table(nome).schema.fields}
        for coluna, desc in tab.colunas.items():
            if coluna not in existentes:
                continue
            comandos.append(
                (
                    f"ALTER TABLE {nome} ALTER COLUMN {coluna} COMMENT '{_q(desc)}'",
                    f"comentario {nome}.{coluna}",
                )
            )
        for coluna in existentes:
            if classificar(coluna) == "confidencial":
                comandos.append(
                    (
                        f"ALTER TABLE {nome} ALTER COLUMN {coluna} SET TAGS ('classificacao' = 'confidencial')",
                        f"tag {nome}.{coluna}",
                    )
                )
        for sql, ctx in comandos:
            if _tentar(spark, cfg, sql, ctx):
                aplicados += 1
            else:
                falhas += 1
    return aplicados, falhas


def aplicar_constraints(spark: SparkSession, cfg: Settings) -> tuple[int, int]:
    """Cria as FKs informativas que ainda nao existem (consulta o information_schema antes)."""
    schema = cfg.schema("gold")
    existentes = {
        r["constraint_name"]
        for r in spark.sql(
            f"SELECT constraint_name FROM {cfg.catalog}.information_schema.table_constraints "
            f"WHERE table_schema = '{schema}'"
        ).collect()
    }
    novas = falhas = 0
    for nome, sql in ddl.fks_gold(cfg):
        if nome in existentes:
            continue
        if _tentar(spark, cfg, sql, f"constraint {nome}"):
            novas += 1
        else:
            falhas += 1
    return novas, falhas


def gerar_dicionario(spark: SparkSession, cfg: Settings) -> int:
    ts = agora()
    linhas = []
    for tab in TABELAS:
        nome = cfg.tabela(tab.camada, tab.nome)
        for campo in spark.table(nome).schema.fields:
            linhas.append(
                (
                    f"{tab.camada}.{tab.nome}",
                    campo.name,
                    campo.dataType.simpleString(),
                    tab.colunas.get(campo.name, ""),
                    tab.camada,
                    classificar(campo.name),
                    ts,
                )
            )
    df = spark.createDataFrame(
        linhas,
        "tabela STRING, coluna STRING, tipo STRING, descricao STRING, camada STRING, "
        "classificacao STRING, atualizado_em TIMESTAMP",
    )
    df.writeTo(cfg.tabela("ctl", "dicionario_dados")).overwrite(F.lit(True))
    return len(linhas)
