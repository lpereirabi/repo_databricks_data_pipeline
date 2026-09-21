"""Regras de qualidade de dados (declarativas) e registro dos resultados.

BLOQUEANTE  -> a linha vai para a quarentena (nao chega na silver).
ALERTA      -> a linha segue, marcada em `dq_alertas`; o resultado fica em `dq_resultado`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .params import Parametros
from .schema import (
    COLUNAS_CAMPANHA,
    COLUNAS_CONTAGEM,
    COLUNAS_GASTO,
    ESCOLARIDADES,
    ESTADOS_CIVIS,
)


@dataclass(frozen=True)
class Regra:
    codigo: str
    descricao: str
    severidade: str  # BLOQUEANTE | ALERTA
    acao: str  # QUARENTENA | SINALIZAR | ANONIMIZAR_CAMPO


REGRAS: list[Regra] = [
    Regra("R001_ID_INVALIDO", "id_cliente nulo ou nao numerico", "BLOQUEANTE", "QUARENTENA"),
    Regra(
        "R002_ID_DUPLICADO",
        "id_cliente repetido na mesma carga (mantem a 1a ocorrencia)",
        "BLOQUEANTE",
        "QUARENTENA",
    ),
    Regra(
        "R003_DATA_CADASTRO_INVALIDA",
        "data_cadastro nula ou fora do formato esperado",
        "BLOQUEANTE",
        "QUARENTENA",
    ),
    Regra("R004_VALOR_NEGATIVO", "gasto, contagem ou salario com valor negativo", "BLOQUEANTE", "QUARENTENA"),
    Regra("A001_SALARIO_AUSENTE", "salario_anual nulo", "ALERTA", "SINALIZAR"),
    Regra("A002_SALARIO_OUTLIER", "salario_anual acima do limite parametrizado", "ALERTA", "SINALIZAR"),
    Regra(
        "A003_ANO_NASCIMENTO_INVALIDO",
        "ano de nascimento ausente ou fora da faixa (vira NULL)",
        "ALERTA",
        "ANONIMIZAR_CAMPO",
    ),
    Regra(
        "A004_POSSIVEL_DUPLICADO",
        "mesmo conteudo (exceto ID) em mais de um cliente na carga",
        "ALERTA",
        "SINALIZAR",
    ),
    Regra("A005_DATA_CADASTRO_FUTURA", "data_cadastro posterior a data da carga", "ALERTA", "SINALIZAR"),
    Regra(
        "A006_DOMINIO_DESCONHECIDO",
        "escolaridade/estado civil fora da lista de dominio",
        "ALERTA",
        "SINALIZAR",
    ),
    Regra("A007_VALOR_NAO_NUMERICO", "campo numerico nao convertido (virou NULL)", "ALERTA", "SINALIZAR"),
]
BLOQUEANTES = [r.codigo for r in REGRAS if r.severidade == "BLOQUEANTE"]
ALERTAS = [r.codigo for r in REGRAS if r.severidade == "ALERTA"]


def _ou(colunas: list[Column]) -> Column:
    return reduce(lambda a, b: a | b, colunas)


def condicoes(params: Parametros, data_carga: date) -> dict[str, Column]:
    """Condicao (True = viola a regra) de cada regra, sobre o DataFrame tipado da silver."""
    numericas = [*COLUNAS_GASTO, *COLUNAS_CONTAGEM, "salario_anual"]
    nulos_numericos = [*COLUNAS_GASTO, *COLUNAS_CONTAGEM, *COLUNAS_CAMPANHA, "comprou"]
    ano_min = params.inteiro("ano_nascimento_min")
    ano_carga = F.year(F.lit(data_carga))
    return {
        "R001_ID_INVALIDO": F.col("id_cliente").isNull(),
        "R002_ID_DUPLICADO": F.col("id_cliente").isNotNull() & (F.col("_seq_id") > 1),
        "R003_DATA_CADASTRO_INVALIDA": F.col("data_cadastro").isNull(),
        "R004_VALOR_NEGATIVO": _ou([F.coalesce(F.col(c) < 0, F.lit(False)) for c in numericas]),
        "A001_SALARIO_AUSENTE": F.col("salario_anual").isNull(),
        "A002_SALARIO_OUTLIER": F.coalesce(
            F.col("salario_anual") > F.lit(params.decimal("salario_max_outlier")), F.lit(False)
        ),
        "A003_ANO_NASCIMENTO_INVALIDO": F.col("ano_nascimento").isNull()
        | (F.col("ano_nascimento") < ano_min)
        | (F.col("ano_nascimento") > ano_carga),
        "A004_POSSIVEL_DUPLICADO": F.col("_n_hash") > 1,
        "A005_DATA_CADASTRO_FUTURA": F.coalesce(F.col("data_cadastro") > F.lit(data_carga), F.lit(False)),
        "A006_DOMINIO_DESCONHECIDO": (
            F.col("escolaridade").isNull()
            | ~F.col("escolaridade").isin(list(ESCOLARIDADES))
            | F.col("estado_civil").isNull()
            | ~F.col("estado_civil").isin(ESTADOS_CIVIS)
        ),
        "A007_VALOR_NAO_NUMERICO": _ou([F.col(c).isNull() for c in nulos_numericos]),
    }


def marcar_regras(df: DataFrame, params: Parametros, data_carga: date) -> DataFrame:
    """Adiciona uma coluna booleana `_r_<codigo>` por regra (True = violou)."""
    for codigo, cond in condicoes(params, data_carga).items():
        df = df.withColumn(f"_r_{codigo}", F.coalesce(cond, F.lit(False)))
    return df


def lista_codigos(codigos: list[str]) -> Column:
    """ARRAY<STRING> com os codigos das regras violadas (vazio se nenhuma)."""
    return F.filter(
        F.array(*[F.when(F.col(f"_r_{c}"), F.lit(c)) for c in codigos]),
        lambda x: x.isNotNull(),
    )


def resumir(df_marcado: DataFrame) -> tuple[int, int, dict[str, int]]:
    """(total de linhas, linhas bloqueadas, {codigo: violacoes}) em uma unica passada.

    Espera a coluna `_bloqueado` (True se qualquer regra bloqueante foi violada).
    """
    agregados = [
        F.count(F.lit(1)).alias("_total"),
        F.sum(F.col("_bloqueado").cast("int")).alias("_bloqueadas"),
        *[F.sum(F.col(f"_r_{r.codigo}").cast("int")).alias(r.codigo) for r in REGRAS],
    ]
    linha = df_marcado.agg(*agregados).first()
    falhas = {r.codigo: int(linha[r.codigo] or 0) for r in REGRAS}
    return int(linha["_total"]), int(linha["_bloqueadas"] or 0), falhas


def gravar_resultados(
    spark: SparkSession, cfg: Settings, data_carga: date, tabela: str, total: int, falhas: dict[str, int]
) -> None:
    agora = datetime.now(timezone.utc)
    linhas = [
        (
            cfg.run_id,
            data_carga,
            tabela,
            r.codigo,
            r.descricao,
            r.severidade,
            r.acao,
            total,
            falhas[r.codigo],
            (falhas[r.codigo] / total * 100.0) if total else 0.0,
            agora,
        )
        for r in REGRAS
    ]
    df = spark.createDataFrame(
        linhas,
        "run_id STRING, data_carga DATE, tabela STRING, regra STRING, descricao STRING, severidade STRING, "
        "acao STRING, total BIGINT, falhas BIGINT, pct_falha DOUBLE, avaliado_ts TIMESTAMP",
    )
    df.writeTo(cfg.tabela("ctl", "dq_resultado")).append()
