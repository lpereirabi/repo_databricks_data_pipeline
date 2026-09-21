"""Camada Bronze: copia fiel do arquivo (tudo STRING) + colunas de auditoria. Sem regra de negocio."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .control import Carga, registrar_erro
from .params import Parametros
from .runner import Resultado, sobrescrever_carga
from .schema import COLUNAS_BRONZE, COLUNAS_META_BRONZE, COLUNAS_ORIGEM


class SchemaInvalidoError(ValueError):
    """O cabecalho do arquivo nao contem todas as colunas esperadas (drift de schema)."""


def padronizar_colunas(df: DataFrame) -> tuple[DataFrame, list[str]]:
    """Remove BOM/espacos do cabecalho, valida o contrato e renomeia para snake_case.

    Retorna (df com colunas padronizadas, colunas extras ignoradas). Coluna faltando = erro.
    """
    limpos = [c.replace("﻿", "").strip() for c in df.columns]
    df = df.toDF(*limpos)
    faltantes = [c for c in COLUNAS_ORIGEM if c not in limpos]
    if faltantes:
        raise SchemaInvalidoError(f"Colunas ausentes no arquivo: {faltantes}")
    extras = [c for c in limpos if c not in COLUNAS_ORIGEM]
    selecao = [F.col(f"`{orig}`").cast("string").alias(novo) for orig, novo in COLUNAS_ORIGEM.items()]
    return df.select(*selecao), extras


def adicionar_metadados(df: DataFrame, data_carga: date, caminho: str, run_id: str) -> DataFrame:
    """Colunas de auditoria/linhagem: de qual arquivo, carga e execucao veio cada linha."""
    conteudo = F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<null>")) for c in COLUNAS_BRONZE])
    return (
        df.withColumn("_data_carga", F.lit(data_carga).cast("date"))
        .withColumn("_arquivo_origem", F.lit(caminho))
        .withColumn("_ingestao_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_seq_linha", F.monotonically_increasing_id())
        .withColumn("_hash_linha", F.sha2(conteudo, 256))
        .select(*COLUNAS_BRONZE, *COLUNAS_META_BRONZE)
    )


def carregar_bronze(spark: SparkSession, cfg: Settings, params: Parametros, carga: Carga) -> Resultado:
    leitura = (
        spark.read.option("header", "true")
        .option("sep", params.texto("csv_separador"))
        .option("encoding", params.texto("csv_encoding"))
        .option("inferSchema", "false")
        .csv(carga.caminho)
    )
    df, extras = padronizar_colunas(leitura)
    if extras:
        registrar_erro(
            spark,
            cfg,
            "bronze",
            "COLUNAS_EXTRAS_IGNORADAS",
            f"Colunas fora do contrato ignoradas: {extras}",
            carga.data_carga,
            severidade="AVISO",
        )
    df = adicionar_metadados(df, carga.data_carga, carga.caminho, cfg.run_id)

    destino = cfg.tabela("bronze", "marketing_raw")
    sobrescrever_carga(df, destino, F.col("_data_carga") == F.lit(carga.data_carga))

    gravadas = spark.table(destino).where(F.col("_data_carga") == F.lit(carga.data_carga)).count()
    if gravadas == 0:
        raise ValueError(f"Arquivo {carga.arquivo} nao possui linhas de dados")
    return Resultado(lidas=gravadas, gravadas=gravadas, campos={"linhas_bronze": gravadas})
