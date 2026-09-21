from datetime import date

import pytest

from mkt_pipeline.files import extrair_data, listar_arquivos, nome_esperado, sha256_arquivo


@pytest.mark.parametrize(
    ("nome", "esperado"),
    [
        ("marketing_20260920.csv", date(2026, 9, 20)),
        ("MARKETING_20260920.CSV", date(2026, 9, 20)),  # caixa nao importa
        ("marketing_20261340.csv", None),  # data impossivel
        ("marketing_2026-09-20.csv", None),  # formato errado
        ("marketing_20260920.csv.bak", None),
        ("vendas_20260920.csv", None),  # prefixo errado
        ("marketing_20260920.txt", None),
    ],
)
def test_extrair_data(nome, esperado):
    assert extrair_data(nome, "marketing", "csv") == esperado


def test_nome_esperado_faz_ida_e_volta():
    nome = nome_esperado("marketing", date(2026, 1, 5), "csv")
    assert nome == "marketing_20260105.csv"
    assert extrair_data(nome, "marketing", "csv") == date(2026, 1, 5)


def test_listar_arquivos_ordena_por_data_e_separa_invalidos(tmp_path):
    for nome in ["marketing_20260921.csv", "marketing_20260920.csv", "lixo.csv", "leia-me.md"]:
        (tmp_path / nome).write_text("x", encoding="utf-8")
    (tmp_path / "subpasta").mkdir()

    validos, invalidos = listar_arquivos(str(tmp_path), "marketing", "csv")

    assert [a.data_carga for a in validos] == [date(2026, 9, 20), date(2026, 9, 21)]
    assert invalidos == ["lixo.csv"]  # .md nem e candidato; pasta e ignorada


def test_listar_arquivos_pasta_inexistente_devolve_vazio(tmp_path):
    assert listar_arquivos(str(tmp_path / "nao_existe"), "marketing", "csv") == ([], [])


def test_sha256_muda_com_o_conteudo(tmp_path):
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    a.write_bytes(b"conteudo 1")
    b.write_bytes(b"conteudo 2")
    assert sha256_arquivo(a) == sha256_arquivo(a)
    assert sha256_arquivo(a) != sha256_arquivo(b)
