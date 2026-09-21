from datetime import date
from decimal import Decimal

import pytest
from helpers import bronze_df

from mkt_pipeline.bronze import SchemaInvalidoError, adicionar_metadados, padronizar_colunas
from mkt_pipeline.params import Parametros
from mkt_pipeline.schema import COLUNAS_BRONZE, COLUNAS_META_BRONZE, COLUNAS_ORIGEM
from mkt_pipeline.silver import transformar_silver

pytestmark = pytest.mark.spark
D = date(2026, 9, 20)


def rodar(spark, linhas):
    ok, quarentena, avaliado = transformar_silver(bronze_df(spark, linhas), Parametros.padrao(), D, "run-1")
    return {r["id_cliente"]: r.asDict() for r in ok.collect()}, quarentena.collect(), avaliado


# ---------------------------------------------------------------- bronze
def test_padronizar_colunas_remove_bom_e_renomeia(spark):
    cabecalho = ["﻿ID"] + [c for c in COLUNAS_ORIGEM if c != "ID"] + ["Coluna Extra"]
    linha = [tuple(str(i) for i in range(len(cabecalho)))]
    df, extras = padronizar_colunas(
        spark.createDataFrame(linha, ", ".join(f"`{c}` STRING" for c in cabecalho))
    )
    assert df.columns == COLUNAS_BRONZE
    assert extras == ["Coluna Extra"]


def test_padronizar_colunas_falha_quando_falta_coluna(spark):
    faltando = [c for c in COLUNAS_ORIGEM if c != "Pais"]
    df = spark.createDataFrame([tuple("x" for _ in faltando)], ", ".join(f"`{c}` STRING" for c in faltando))
    with pytest.raises(SchemaInvalidoError, match="Pais"):
        padronizar_colunas(df)


def test_metadados_de_auditoria(spark):
    df = adicionar_metadados(
        bronze_df(spark, [{}, {"id_cliente": "2"}]).drop("_seq_linha"), D, "/v/a.csv", "run-1"
    )
    assert df.columns == COLUNAS_BRONZE + COLUNAS_META_BRONZE
    r = df.first()
    assert (r["_data_carga"], r["_arquivo_origem"], r["_run_id"]) == (D, "/v/a.csv", "run-1")
    assert len(r["_hash_linha"]) == 64
    assert df.select("_seq_linha").distinct().count() == 2  # sequencial unico por linha


# ---------------------------------------------------------------- silver: tipagem
def test_tipagem_e_metricas_derivadas(spark):
    ok, quarentena, _ = rodar(spark, [{}])
    r = ok[1]
    assert quarentena == []
    assert r["data_cadastro"] == date(2021, 7, 6)  # 6/07/2021 = dia/mes/ano
    assert r["salario_anual"] == Decimal("50000.00")
    assert r["total_gasto"] == Decimal("200.00")  # 100+10+50+5+20+15
    assert r["total_compras"] == 8  # web 3 + catalogo 1 + loja 4 (desconto NAO soma)
    assert r["qtd_campanhas_aceitas"] == 2
    assert r["comprou"] is True and r["campanha_2_aceita"] is False
    assert r["dq_alertas"] == []


def test_data_com_dia_de_dois_digitos(spark):
    ok, _, _ = rodar(spark, [{"data_cadastro": "07/01/2020"}])
    assert ok[1]["data_cadastro"] == date(2020, 1, 7)


# ---------------------------------------------------------------- silver: regras bloqueantes -> quarentena
@pytest.mark.parametrize(
    ("campos", "regra"),
    [
        ({"id_cliente": "abc"}, "R001_ID_INVALIDO"),
        ({"id_cliente": ""}, "R001_ID_INVALIDO"),
        ({"data_cadastro": "31/02/2022"}, "R003_DATA_CADASTRO_INVALIDA"),
        ({"data_cadastro": ""}, "R003_DATA_CADASTRO_INVALIDA"),
        ({"gasto_moveis": "-50"}, "R004_VALOR_NEGATIVO"),
        ({"qtd_compras_web": "-1"}, "R004_VALOR_NEGATIVO"),
    ],
)
def test_linha_invalida_vai_para_quarentena_com_motivo(spark, campos, regra):
    ok, quarentena, _ = rodar(spark, [{"id_cliente": "1"}, {"id_cliente": "2", **campos}])
    assert list(ok) == [1]
    assert len(quarentena) == 1
    assert regra in quarentena[0]["motivos"]
    assert '"Pais":' not in quarentena[0]["linha_original"]  # o JSON usa os nomes padronizados
    assert '"pais":"Brasil"' in quarentena[0]["linha_original"]


def test_id_duplicado_mantem_a_primeira_ocorrencia(spark):
    ok, quarentena, _ = rodar(
        spark, [{"id_cliente": "7", "pais": "Chile"}, {"id_cliente": "7", "pais": "Peru"}]
    )
    assert ok[7]["pais"] == "Chile"
    assert quarentena[0]["motivos"] == ["R002_ID_DUPLICADO"]


# ---------------------------------------------------------------- silver: alertas (linha segue sinalizada)
def test_alertas_nao_removem_a_linha(spark):
    ok, quarentena, _ = rodar(
        spark,
        [
            {"id_cliente": "1", "salario_anual": ""},  # A001
            {"id_cliente": "2", "salario_anual": "666666"},  # A002 (limite 300000)
            {"id_cliente": "3", "ano_nascimento": "1893"},  # A003
            {"id_cliente": "4", "escolaridade": "Pos-doutorado inventado"},  # A006
            {"id_cliente": "5", "data_cadastro": "01/01/2030"},  # A005 (futura)
            {"id_cliente": "6", "gasto_vestuario": "abc"},  # A007
        ],
    )
    assert quarentena == []
    assert ok[1]["dq_alertas"] == ["A001_SALARIO_AUSENTE"] and ok[1]["salario_anual"] is None
    assert ok[2]["dq_alertas"] == ["A002_SALARIO_OUTLIER"]
    assert ok[3]["dq_alertas"] == ["A003_ANO_NASCIMENTO_INVALIDO"] and ok[3]["ano_nascimento"] is None
    assert ok[4]["dq_alertas"] == ["A006_DOMINIO_DESCONHECIDO"]
    assert ok[5]["dq_alertas"] == ["A005_DATA_CADASTRO_FUTURA"]
    assert ok[6]["dq_alertas"] == ["A007_VALOR_NAO_NUMERICO"]


def test_clientes_clonados_sao_sinalizados(spark):
    ok, _, _ = rodar(
        spark, [{"id_cliente": "1"}, {"id_cliente": "2"}, {"id_cliente": "3", "salario_anual": "1"}]
    )
    assert (
        "A004_POSSIVEL_DUPLICADO" in ok[1]["dq_alertas"] and "A004_POSSIVEL_DUPLICADO" in ok[2]["dq_alertas"]
    )
    assert "A004_POSSIVEL_DUPLICADO" not in ok[3]["dq_alertas"]


def test_parametro_altera_o_limite_do_outlier(spark):
    params = Parametros({**Parametros.padrao().valores, "salario_max_outlier": ("40000", "decimal")})
    ok, _, _ = transformar_silver(bronze_df(spark, [{}]), params, D, "run-1")[:3]
    assert ok.first()["dq_alertas"] == ["A002_SALARIO_OUTLIER"]  # 50000 > 40000
