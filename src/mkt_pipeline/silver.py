"""Camada Silver: tipagem, limpeza, qualidade (regras + quarentena) e metricas derivadas."""

from __future__ import annotations

from datetime import date
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import Settings
from .control import Carga
from .dq import ALERTAS, BLOQUEANTES, gravar_resultados, lista_codigos, marcar_regras, resumir
from .params import Parametros
from .runner import Resultado, sobrescrever_carga
from .schema import (
    COLUNAS_BRONZE,
    COLUNAS_CAMPANHA,
    COLUNAS_CANAL,
    COLUNAS_CONTAGEM,
    COLUNAS_GASTO,
    NAO_INFORMADO,
)


class LimiteRejeicaoExcedido(RuntimeError):
    """Percentual de linhas em quarentena acima do limite parametrizado: a carga nao avanca."""


def _tc(coluna: str, tipo: str) -> Column:
    """try_cast: valor invalido vira NULL em vez de derrubar a query (o ANSI mode do serverless lancaria erro)."""
    return F.expr(f"try_cast(trim(`{coluna}`) AS {tipo})")


def _txt(coluna: str) -> Column:
    return F.when(F.trim(F.col(coluna)) == "", None).otherwise(F.trim(F.col(coluna)))


def _flag(coluna: str) -> Column:
    return F.expr(f"try_cast(trim(`{coluna}`) AS INT) = 1")


def tipar(bronze: DataFrame, params: Parametros) -> DataFrame:
    """Converte a bronze (STRING) para os tipos corretos. Valores invalidos viram NULL (as regras os detectam)."""
    fmt = params.texto("formato_data_origem").replace("'", "''")
    colunas: list[Column] = [
        _tc("id_cliente", "BIGINT").alias("id_cliente"),
        _tc("ano_nascimento", "INT").alias("ano_nascimento"),
        _txt("escolaridade").alias("escolaridade"),
        _txt("estado_civil").alias("estado_civil"),
        _txt("pais").alias("pais"),
        _tc("salario_anual", "DECIMAL(12,2)").alias("salario_anual"),
        F.expr(f"CAST(try_to_timestamp(trim(`data_cadastro`), '{fmt}') AS DATE)").alias("data_cadastro"),
        *[_tc(c, "INT").alias(c) for c in COLUNAS_CONTAGEM],
        *[_tc(c, "DECIMAL(12,2)").alias(c) for c in COLUNAS_GASTO],
        *[_flag(c).alias(c) for c in COLUNAS_CAMPANHA],
        _flag("comprou").alias("comprou"),
        F.col("_seq_linha"),
        F.col("id_cliente").alias("_id_raw"),
        F.to_json(F.struct(*[F.col(c) for c in COLUNAS_BRONZE])).alias("_linha_original"),
    ]
    return bronze.select(*colunas)


def _com_apoio(tipado: DataFrame) -> DataFrame:
    """Colunas auxiliares das regras: ordem dentro do ID e hash do conteudo (exceto ID) para achar clones."""
    partes = [
        F.coalesce(F.col(c).cast("string"), F.lit("<null>"))
        for c in tipado.columns
        if not c.startswith("_") and c != "id_cliente"
    ]
    return (
        tipado.withColumn(
            "_seq_id", F.row_number().over(Window.partitionBy("id_cliente").orderBy("_seq_linha"))
        )
        .withColumn("hash_conteudo", F.sha2(F.concat_ws("|", *partes), 256))
        .withColumn("_n_hash", F.count(F.lit(1)).over(Window.partitionBy("hash_conteudo")))
    )


def transformar_silver(
    bronze: DataFrame, params: Parametros, data_carga: date, run_id: str
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Funcao pura (sem I/O). Retorna (silver_ok, quarentena, avaliado) onde `avaliado` tem as flags _r_*."""
    avaliado = marcar_regras(_com_apoio(tipar(bronze, params)), params, data_carga).withColumn(
        "_bloqueado", F.size(lista_codigos(BLOQUEANTES)) > 0
    )
    bloqueado = F.col("_bloqueado")
    ts = F.current_timestamp()
    dt = F.lit(data_carga).cast("date")

    ano_limpo = F.when(F.col("_r_A003_ANO_NASCIMENTO_INVALIDO"), F.lit(None).cast("int")).otherwise(
        F.col("ano_nascimento")
    )
    zero = F.lit(0)
    total_gasto = reduce(lambda a, b: a + b, [F.coalesce(F.col(c), F.lit(0)) for c in COLUNAS_GASTO])
    total_compras = reduce(lambda a, b: a + b, [F.coalesce(F.col(c), zero) for c in COLUNAS_CANAL])
    qtd_campanhas = reduce(
        lambda a, b: a + b, [F.coalesce(F.col(c).cast("int"), zero) for c in COLUNAS_CAMPANHA]
    )

    ok = avaliado.where(~bloqueado).select(
        "id_cliente",
        ano_limpo.alias("ano_nascimento"),
        F.coalesce(F.col("escolaridade"), F.lit(NAO_INFORMADO)).alias("escolaridade"),
        F.coalesce(F.col("estado_civil"), F.lit(NAO_INFORMADO)).alias("estado_civil"),
        F.coalesce(F.col("pais"), F.lit(NAO_INFORMADO)).alias("pais"),
        "salario_anual",
        "qtd_filhos",
        "qtd_adolescentes",
        "data_cadastro",
        "dias_desde_ultima_compra",
        *COLUNAS_GASTO,
        total_gasto.cast("decimal(14,2)").alias("total_gasto"),
        "qtd_compras_desconto",
        *COLUNAS_CANAL,
        total_compras.cast("int").alias("total_compras"),
        "visitas_website_mes",
        *COLUNAS_CAMPANHA,
        qtd_campanhas.cast("int").alias("qtd_campanhas_aceitas"),
        "comprou",
        "hash_conteudo",
        lista_codigos(ALERTAS).alias("dq_alertas"),
        dt.alias("_data_carga"),
        F.lit(run_id).alias("_run_id"),
        ts.alias("_processado_ts"),
    )
    quarentena = avaliado.where(bloqueado).select(
        F.col("_id_raw").alias("id_cliente_raw"),
        lista_codigos(BLOQUEANTES).alias("motivos"),
        F.col("_linha_original").alias("linha_original"),
        dt.alias("_data_carga"),
        F.lit(run_id).alias("_run_id"),
        ts.alias("_processado_ts"),
    )
    return ok, quarentena, avaliado


def processar_silver(spark: SparkSession, cfg: Settings, params: Parametros, carga: Carga) -> Resultado:
    origem = spark.table(cfg.tabela("bronze", "marketing_raw")).where(
        F.col("_data_carga") == F.lit(carga.data_carga)
    )
    ok, quarentena, avaliado = transformar_silver(origem, params, carga.data_carga, cfg.run_id)

    total, n_quarentena, falhas = resumir(avaliado)
    gravar_resultados(spark, cfg, carga.data_carga, "silver.marketing_cliente", total, falhas)
    n_ok = total - n_quarentena

    cond = F.col("_data_carga") == F.lit(carga.data_carga)
    sobrescrever_carga(quarentena, cfg.tabela("silver", "marketing_quarentena"), cond)

    limite = float(params.decimal("limite_rejeicao_pct"))
    pct = (n_quarentena / total * 100.0) if total else 0.0
    if pct > limite:
        raise LimiteRejeicaoExcedido(
            f"{n_quarentena}/{total} linhas em quarentena ({pct:.1f}%) acima do limite de {limite:.1f}%"
        )

    sobrescrever_carga(ok, cfg.tabela("silver", "marketing_cliente"), cond)
    return Resultado(
        lidas=total,
        gravadas=n_ok,
        rejeitadas=n_quarentena,
        mensagem=f"{pct:.2f}% em quarentena",
        campos={"linhas_silver": n_ok, "linhas_quarentena": n_quarentena},
    )
