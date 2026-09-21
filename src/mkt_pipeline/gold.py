"""Camada Gold: modelo dimensional (estrela) para consumo em BI.

Dimensoes: cliente (SCD Tipo 2), data, pais, escolaridade, estado civil, categoria, canal, campanha.
Fatos:     cliente_snapshot, gasto_categoria, compras_canal, resposta_campanha.

Chaves substitutas das dimensoes pequenas sao **hash deterministico** (xxhash64) do valor de negocio: o
mesmo valor gera a mesma chave em qualquer execucao/ambiente, sem tabela de sequencia e sem lookup.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .control import Carga
from .params import Parametros
from .runner import Resultado, sobrescrever_carga
from .schema import (
    ATRIBUTOS_SCD2,
    CAMPANHAS,
    CANAIS,
    CATEGORIAS,
    ESCOLARIDADES,
    NAO_INFORMADO,
)

FIM_VIGENCIA = "9999-12-31"

_MESES = [
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
]
_DIAS = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]


def sk_data(coluna: Column) -> Column:
    """Chave da dimensao de calendario: AAAAMMDD como inteiro."""
    return F.date_format(coluna, "yyyyMMdd").cast("int")


# --------------------------------------------------------------------------------------------------
# Dimensoes
# --------------------------------------------------------------------------------------------------
def construir_dim_data(spark: SparkSession, inicio: str, fim: str) -> DataFrame:
    base = spark.sql(f"SELECT explode(sequence(DATE'{inicio}', DATE'{fim}', INTERVAL 1 DAY)) AS data")
    meses = F.array(*[F.lit(m) for m in _MESES])
    dias = F.array(*[F.lit(d) for d in _DIAS])
    dia_semana = F.weekday("data") + 1  # 1 = segunda ... 7 = domingo (ISO)
    return base.select(
        sk_data(F.col("data")).alias("sk_data"),
        "data",
        F.year("data").alias("ano"),
        F.when(F.month("data") <= 6, 1).otherwise(2).alias("semestre"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        F.element_at(meses, F.month("data")).alias("nome_mes"),
        F.date_format("data", "yyyy-MM").alias("ano_mes"),
        F.dayofmonth("data").alias("dia"),
        dia_semana.alias("dia_semana"),
        F.element_at(dias, dia_semana).alias("nome_dia_semana"),
        F.weekofyear("data").alias("semana_ano"),
        (dia_semana >= 6).alias("fim_de_semana"),
    )


def _merge_insert_novos(
    spark: SparkSession, destino: str, origem: DataFrame, chave: str, atualizar: bool = False
) -> None:
    origem.createOrReplaceTempView("_dim_origem")
    quando_igual = "WHEN MATCHED THEN UPDATE SET *" if atualizar else ""
    spark.sql(
        f"""MERGE INTO {destino} AS t USING _dim_origem AS s ON t.{chave} = s.{chave}
{quando_igual}
WHEN NOT MATCHED THEN INSERT *"""
    )


def semear_dimensoes_estaticas(spark: SparkSession, cfg: Settings, params: Parametros) -> None:
    """Dimensoes cujo dominio e fixo (categoria, canal, campanha) e o calendario. Idempotente."""
    g = lambda n: cfg.tabela("gold", n)  # noqa: E731
    estaticas = {
        "dim_categoria_produto": ("cod_categoria", "categoria", "sk_categoria", CATEGORIAS),
        "dim_canal": ("cod_canal", "canal", "sk_canal", CANAIS),
        "dim_campanha": ("cod_campanha", "campanha", "sk_campanha", CAMPANHAS),
    }
    for tabela, (col_cod, col_nome, col_sk, dominio) in estaticas.items():
        linhas = [(cod, nome) for cod, (nome, _) in dominio.items()]
        df = spark.createDataFrame(linhas, f"{col_cod} STRING, {col_nome} STRING").select(
            F.xxhash64(F.col(col_cod)).alias(col_sk), col_cod, col_nome
        )
        _merge_insert_novos(spark, g(tabela), df, col_sk, atualizar=True)

    if spark.table(g("dim_data")).limit(1).count() == 0:
        dim = construir_dim_data(spark, params.texto("dim_data_inicio"), params.texto("dim_data_fim"))
        dim.writeTo(g("dim_data")).append()


def atualizar_dimensoes_simples(spark: SparkSession, cfg: Settings, silver: DataFrame) -> None:
    """Pais, escolaridade e estado civil: acrescenta valores novos vistos na silver (nunca remove)."""
    g = lambda n: cfg.tabela("gold", n)  # noqa: E731
    nao_inf = spark.createDataFrame([(NAO_INFORMADO,)], "valor STRING")

    def distintos(col: str) -> DataFrame:
        return (
            silver.select(F.col(col).alias("valor"))
            .where(F.col("valor").isNotNull())
            .distinct()
            .unionByName(nao_inf)
            .distinct()
        )

    pais = distintos("pais").select(F.xxhash64("valor").alias("sk_pais"), F.col("valor").alias("pais"))
    _merge_insert_novos(spark, g("dim_pais"), pais, "sk_pais")

    estado = distintos("estado_civil").select(
        F.xxhash64("valor").alias("sk_estado_civil"), F.col("valor").alias("estado_civil")
    )
    _merge_insert_novos(spark, g("dim_estado_civil"), estado, "sk_estado_civil")

    ordem = F.create_map(*[x for nome, n in ESCOLARIDADES.items() for x in (F.lit(nome), F.lit(n))])
    esc = distintos("escolaridade").select(
        F.xxhash64("valor").alias("sk_escolaridade"),
        F.col("valor").alias("escolaridade"),
        # try_element_at: chave ausente vira NULL (com ANSI ligado, ordem[chave] lancaria erro)
        F.coalesce(F.try_element_at(ordem, F.col("valor")), F.lit(99)).cast("int").alias("nivel_ordem"),
    )
    _merge_insert_novos(spark, g("dim_escolaridade"), esc, "sk_escolaridade")


def faixa_salarial(params: Parametros) -> Column:
    sal = F.col("salario_anual")
    return (
        F.when(sal.isNull(), F.lit(NAO_INFORMADO))
        .when(sal <= F.lit(params.decimal("faixa_salarial_baixa_ate")), F.lit("Baixa"))
        .when(sal <= F.lit(params.decimal("faixa_salarial_media_ate")), F.lit("Média"))
        .when(sal <= F.lit(params.decimal("faixa_salarial_alta_ate")), F.lit("Alta"))
        .otherwise(F.lit("Muito Alta"))
    )


def origem_scd2(silver_carga: DataFrame, params: Parametros) -> DataFrame:
    """Linhas da carga com os atributos da dimensao cliente e o hash dos atributos rastreados."""
    tracked = F.concat_ws(
        "|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in ATRIBUTOS_SCD2]
    )
    return silver_carga.select(
        "id_cliente",
        "ano_nascimento",
        "escolaridade",
        "estado_civil",
        "pais",
        "salario_anual",
        faixa_salarial(params).alias("faixa_salarial"),
        "qtd_filhos",
        "qtd_adolescentes",
        "data_cadastro",
        F.sha2(tracked, 256).alias("hash_atributos"),
    )


_ATRIBUTOS_DIM = [
    "ano_nascimento",
    "escolaridade",
    "estado_civil",
    "pais",
    "salario_anual",
    "faixa_salarial",
    "qtd_filhos",
    "qtd_adolescentes",
    "data_cadastro",
]


def sql_merge_scd2(dim: str, data_carga: date, run_id: str) -> str:
    """MERGE SCD Tipo 2 (view de origem: `_src_cliente`).

    Padrao "duas linhas por chave alterada": uma com merge_key = id (fecha a versao antiga) e outra com
    merge_key NULL (nao casa com nada e vira INSERT da nova versao). Reprocessar a MESMA data corrige a
    versao no lugar (inicio_vigencia = data) em vez de criar uma versao com vigencia invertida.
    """
    d = data_carga.isoformat()
    set_attrs = ", ".join(f"t.{c} = m.{c}" for c in _ATRIBUTOS_DIM)
    cols_ins = ", ".join(_ATRIBUTOS_DIM)
    vals_ins = ", ".join(f"m.{c}" for c in _ATRIBUTOS_DIM)
    return f"""MERGE INTO {dim} AS t
USING (
  SELECT s.id_cliente AS merge_key, s.* FROM _src_cliente s
  UNION ALL
  SELECT CAST(NULL AS BIGINT) AS merge_key, s.*
  FROM _src_cliente s JOIN {dim} d
    ON s.id_cliente = d.id_cliente AND d.flag_atual = true
  WHERE s.hash_atributos <> d.hash_atributos AND d.inicio_vigencia < DATE'{d}'
) AS m
ON t.id_cliente = m.merge_key AND t.flag_atual = true
WHEN MATCHED AND t.hash_atributos <> m.hash_atributos AND t.inicio_vigencia = DATE'{d}' THEN UPDATE SET
  {set_attrs}, t.hash_atributos = m.hash_atributos, t._run_id = '{run_id}', t._atualizado_ts = current_timestamp()
WHEN MATCHED AND t.hash_atributos <> m.hash_atributos THEN UPDATE SET
  t.flag_atual = false, t.fim_vigencia = date_sub(DATE'{d}', 1), t._run_id = '{run_id}', t._atualizado_ts = current_timestamp()
WHEN NOT MATCHED THEN INSERT (sk_cliente, id_cliente, {cols_ins}, hash_atributos, inicio_vigencia, fim_vigencia,
  flag_atual, _run_id, _atualizado_ts)
VALUES (xxhash64(m.id_cliente, CAST(DATE'{d}' AS STRING)), m.id_cliente, {vals_ins}, m.hash_atributos,
  DATE'{d}', DATE'{FIM_VIGENCIA}', true, '{run_id}', current_timestamp())"""


def processar_dimensoes(spark: SparkSession, cfg: Settings, params: Parametros, carga: Carga) -> Resultado:
    silver = spark.table(cfg.tabela("silver", "marketing_cliente")).where(
        F.col("_data_carga") == F.lit(carga.data_carga)
    )
    atualizar_dimensoes_simples(spark, cfg, silver)
    origem_scd2(silver, params).createOrReplaceTempView("_src_cliente")
    spark.sql(sql_merge_scd2(cfg.tabela("gold", "dim_cliente"), carga.data_carga, cfg.run_id))
    n = silver.count()
    return Resultado(lidas=n, gravadas=n, mensagem="dimensoes atualizadas")


# --------------------------------------------------------------------------------------------------
# Fatos
# --------------------------------------------------------------------------------------------------
def _desempilhar(
    df: DataFrame,
    dominio: dict[str, tuple[str, str]],
    colunas_fixas: list[str],
    nome_valor: str,
    sk_nome: str,
) -> DataFrame:
    """Transforma colunas em linhas (unpivot) com STACK e gera a chave da dimensao pelo codigo."""
    pares = ", ".join(f"'{cod}', `{coluna}`" for cod, (_, coluna) in dominio.items())
    empilhado = df.select(*colunas_fixas, F.expr(f"stack({len(dominio)}, {pares}) AS (_cod, _valor)"))
    return empilhado.select(
        *colunas_fixas, F.xxhash64("_cod").alias(sk_nome), F.col("_valor").alias(nome_valor)
    )


def faixa_etaria(idade: Column) -> Column:
    return (
        F.when(idade.isNull(), F.lit(NAO_INFORMADO))
        .when(idade < 40, F.lit("Até 39"))
        .when(idade < 50, F.lit("40-49"))
        .when(idade < 60, F.lit("50-59"))
        .when(idade < 70, F.lit("60-69"))
        .otherwise(F.lit("70+"))
    )


def construir_fatos(
    silver: DataFrame, dim_vigente: DataFrame, data_carga: date, run_id: str
) -> dict[str, DataFrame]:
    """Funcao pura: recebe silver da carga e a dimensao cliente vigente na data; devolve os 4 fatos."""
    base = silver.join(dim_vigente.select("sk_cliente", "id_cliente"), "id_cliente", "inner")
    sk_carga = int(data_carga.strftime("%Y%m%d"))
    meta = [F.lit(run_id).alias("_run_id"), F.current_timestamp().alias("_carga_ts")]
    idade = F.year(F.lit(data_carga)) - F.col("ano_nascimento")

    snapshot = base.select(
        "sk_cliente",
        "id_cliente",
        F.lit(sk_carga).alias("sk_data_carga"),
        sk_data(F.col("data_cadastro")).alias("sk_data_cadastro"),
        F.xxhash64("pais").alias("sk_pais"),
        F.xxhash64("escolaridade").alias("sk_escolaridade"),
        F.xxhash64("estado_civil").alias("sk_estado_civil"),
        idade.cast("int").alias("idade"),
        faixa_etaria(idade).alias("faixa_etaria"),
        "salario_anual",
        "dias_desde_ultima_compra",
        "visitas_website_mes",
        "qtd_compras_desconto",
        "qtd_compras_web",
        "qtd_compras_catalogo",
        "qtd_compras_loja",
        "total_compras",
        "total_gasto",
        "qtd_campanhas_aceitas",
        F.col("comprou").cast("int").alias("comprou"),
        *meta,
    )
    fixas = ["sk_cliente", "sk_data_carga"]
    com_sk = base.withColumn("sk_data_carga", F.lit(sk_carga))

    gasto = _desempilhar(com_sk, CATEGORIAS, fixas, "valor_gasto", "sk_categoria").select(
        *fixas, "sk_categoria", "valor_gasto", *meta
    )
    canal = _desempilhar(com_sk, CANAIS, fixas, "qtd_compras", "sk_canal").select(
        *fixas, "sk_canal", "qtd_compras", *meta
    )
    camp = _desempilhar(com_sk, CAMPANHAS, fixas, "aceitou", "sk_campanha").select(
        *fixas, "sk_campanha", F.col("aceitou").cast("int").alias("aceitou"), *meta
    )
    return {
        "fato_cliente_snapshot": snapshot,
        "fato_gasto_categoria": gasto,
        "fato_compras_canal": canal,
        "fato_resposta_campanha": camp,
    }


def processar_fatos(spark: SparkSession, cfg: Settings, carga: Carga) -> Resultado:
    silver = spark.table(cfg.tabela("silver", "marketing_cliente")).where(
        F.col("_data_carga") == F.lit(carga.data_carga)
    )
    d = F.lit(carga.data_carga)
    dim_vigente = spark.table(cfg.tabela("gold", "dim_cliente")).where(
        (F.col("inicio_vigencia") <= d) & (d <= F.col("fim_vigencia"))
    )
    fatos = construir_fatos(silver, dim_vigente, carga.data_carga, cfg.run_id)

    esperado = silver.count()
    casadas = fatos["fato_cliente_snapshot"].count()
    if casadas != esperado:
        raise ValueError(
            f"Integridade: {esperado - casadas} cliente(s) da silver sem versao vigente em dim_cliente "
            f"(silver={esperado}, snapshot={casadas})"
        )

    cond = F.col("sk_data_carga") == F.lit(int(carga.data_carga.strftime("%Y%m%d")))
    gravadas = 0
    for nome, df in fatos.items():
        sobrescrever_carga(df, cfg.tabela("gold", nome), cond)
        gravadas += spark.table(cfg.tabela("gold", nome)).where(cond).count()
    return Resultado(lidas=esperado, gravadas=gravadas, mensagem=f"{len(fatos)} fatos atualizados")
