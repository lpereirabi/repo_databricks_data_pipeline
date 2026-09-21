from datetime import date

import pytest
from helpers import bronze_df
from pyspark.sql import functions as F

from mkt_pipeline.gold import (
    construir_dim_data,
    construir_fatos,
    faixa_etaria,
    faixa_salarial,
    origem_scd2,
    sql_merge_scd2,
)
from mkt_pipeline.params import Parametros
from mkt_pipeline.silver import transformar_silver

pytestmark = pytest.mark.spark
D = date(2026, 9, 20)


def silver_e_dim(spark, linhas):
    silver, _, _ = transformar_silver(bronze_df(spark, linhas), Parametros.padrao(), D, "run-1")
    dim = silver.select("id_cliente").withColumn("sk_cliente", F.xxhash64("id_cliente", F.lit("2026-09-20")))
    return silver, dim


def test_fatos_tem_a_granularidade_e_os_totais_certos(spark):
    silver, dim = silver_e_dim(spark, [{"id_cliente": "1"}, {"id_cliente": "2", "gasto_moveis": "60"}])
    fatos = construir_fatos(silver, dim, D, "run-1")

    assert fatos["fato_cliente_snapshot"].count() == 2
    assert fatos["fato_gasto_categoria"].count() == 2 * 6
    assert fatos["fato_compras_canal"].count() == 2 * 3
    assert fatos["fato_resposta_campanha"].count() == 2 * 5

    # conciliacao: soma dos gastos por categoria == total_gasto do snapshot
    soma_cat = fatos["fato_gasto_categoria"].agg(F.sum("valor_gasto")).first()[0]
    soma_snap = fatos["fato_cliente_snapshot"].agg(F.sum("total_gasto")).first()[0]
    assert soma_cat == soma_snap == 200 + 210

    snap = fatos["fato_cliente_snapshot"].where("id_cliente = 1").first()
    assert snap["sk_data_carga"] == 20260920 and snap["sk_data_cadastro"] == 20210706
    assert snap["idade"] == 56 and snap["faixa_etaria"] == "50-59"  # 2026 - 1970
    assert snap["comprou"] == 1


def test_chaves_das_dimensoes_pequenas_sao_hash_do_valor_de_negocio(spark):
    silver, dim = silver_e_dim(spark, [{"id_cliente": "1"}])
    fatos = construir_fatos(silver, dim, D, "run-1")
    esperado = spark.range(1).select(F.xxhash64(F.lit("ELETRONICOS")).alias("sk")).first()["sk"]
    gasto_eletronicos = fatos["fato_gasto_categoria"].where(f"sk_categoria = {esperado}").first()
    assert gasto_eletronicos["valor_gasto"] == 100
    pais = spark.range(1).select(F.xxhash64(F.lit("Brasil")).alias("sk")).first()["sk"]
    assert fatos["fato_cliente_snapshot"].first()["sk_pais"] == pais


def test_cliente_sem_versao_vigente_na_dim_fica_de_fora_do_snapshot(spark):
    silver, dim = silver_e_dim(spark, [{"id_cliente": "1"}, {"id_cliente": "2", "gasto_moveis": "60"}])
    fatos = construir_fatos(silver, dim.where("id_cliente = 1"), D, "run-1")
    # processar_fatos compara este count com o da silver e aborta a carga se divergir
    assert fatos["fato_cliente_snapshot"].count() == 1


def test_hash_scd2_muda_so_com_atributos_rastreados(spark):
    silver, _, _ = transformar_silver(
        bronze_df(
            spark,
            [
                {"id_cliente": "1"},
                {"id_cliente": "2", "gasto_moveis": "999"},  # metrica: NAO e atributo da dimensao
                {"id_cliente": "3", "salario_anual": "51000"},  # atributo rastreado
                {"id_cliente": "4", "estado_civil": "Solteiro"},  # atributo rastreado
            ],
        ),
        Parametros.padrao(),
        D,
        "run-1",
    )
    h = {r["id_cliente"]: r["hash_atributos"] for r in origem_scd2(silver, Parametros.padrao()).collect()}
    assert h[1] == h[2]
    assert h[1] != h[3] and h[1] != h[4] and h[3] != h[4]


def test_faixas(spark):
    salarios = spark.createDataFrame(
        [(None,), (10000,), (30000,), (45000,), (80000,), (500000,)], "salario_anual INT"
    )
    faixas = [r["f"] for r in salarios.select(faixa_salarial(Parametros.padrao()).alias("f")).collect()]
    assert faixas == ["Não Informado", "Baixa", "Baixa", "Média", "Alta", "Muito Alta"]

    idades = spark.createDataFrame([(None,), (30,), (39,), (40,), (59,), (60,), (70,)], "i INT")
    assert [r["f"] for r in idades.select(faixa_etaria(F.col("i")).alias("f")).collect()] == [
        "Não Informado", "Até 39", "Até 39", "40-49", "50-59", "60-69", "70+",
    ]  # fmt: skip


def test_dim_data(spark):
    df = construir_dim_data(spark, "2026-09-19", "2026-09-21")
    linhas = {r["data"]: r for r in df.collect()}
    assert sorted(r["sk_data"] for r in linhas.values()) == [20260919, 20260920, 20260921]
    sabado = linhas[date(2026, 9, 19)]
    assert (sabado["nome_dia_semana"], sabado["dia_semana"], sabado["fim_de_semana"]) == ("Sábado", 6, True)
    domingo = linhas[date(2026, 9, 20)]
    assert (domingo["nome_mes"], domingo["trimestre"], domingo["semestre"]) == ("Setembro", 3, 2)
    assert linhas[date(2026, 9, 21)]["fim_de_semana"] is False


# ---------------------------------------------------------------- SQL do SCD2 (o MERGE em si roda no smoke test do STG)
def test_sql_scd2_fecha_versao_antiga_e_corrige_reprocesso_no_mesmo_dia():
    sql = sql_merge_scd2("c.s.dim_cliente", D, "run-9")
    assert "fim_vigencia = date_sub(DATE'2026-09-20', 1)" in sql  # fecha a versao anterior
    assert (
        "d.inicio_vigencia < DATE'2026-09-20'" in sql
    )  # nao cria versao invertida ao reprocessar a mesma data
    assert "t.inicio_vigencia = DATE'2026-09-20' THEN UPDATE SET" in sql  # corrige no lugar
    assert "DATE'9999-12-31', true" in sql and "'run-9'" in sql
    assert sql.index("inicio_vigencia = DATE'2026-09-20' THEN") < sql.index(
        "t.flag_atual = false"
    )  # ordem das clausulas
