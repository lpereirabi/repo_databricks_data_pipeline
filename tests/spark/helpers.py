"""Massa de teste: linhas da bronze (tudo STRING) com um cliente valido como base."""

from __future__ import annotations

from mkt_pipeline.schema import COLUNAS_BRONZE

CLIENTE_VALIDO = {
    "id_cliente": "1",
    "ano_nascimento": "1970",
    "escolaridade": "Mestrado",
    "estado_civil": "Casado",
    "salario_anual": "50000",
    "qtd_filhos": "1",
    "qtd_adolescentes": "0",
    "data_cadastro": "6/07/2021",  # dia com 1 digito, como no arquivo real
    "dias_desde_ultima_compra": "10",
    "gasto_eletronicos": "100",
    "gasto_brinquedos": "10",
    "gasto_moveis": "50",
    "gasto_utilidades": "5",
    "gasto_alimentos": "20",
    "gasto_vestuario": "15",
    "qtd_compras_desconto": "2",
    "qtd_compras_web": "3",
    "qtd_compras_catalogo": "1",
    "qtd_compras_loja": "4",
    "visitas_website_mes": "7",
    "campanha_1_aceita": "1",
    "campanha_2_aceita": "0",
    "campanha_3_aceita": "0",
    "campanha_4_aceita": "1",
    "campanha_5_aceita": "0",
    "comprou": "1",
    "pais": "Brasil",
}


def bronze_df(spark, linhas: list[dict]):
    """Cada dict sobrescreve campos de CLIENTE_VALIDO. Dois clientes "iguais" precisam de conteudo distinto
    para nao acionar a regra de possivel duplicado."""
    registros = []
    for seq, sobrescrita in enumerate(linhas):
        base = {**CLIENTE_VALIDO, **sobrescrita}
        registros.append((*[base[c] for c in COLUNAS_BRONZE], seq))
    return spark.createDataFrame(
        registros, ", ".join(f"{c} STRING" for c in COLUNAS_BRONZE) + ", _seq_linha LONG"
    )
