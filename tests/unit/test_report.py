from datetime import datetime, timezone

from mkt_pipeline.report import (
    barras,
    delta_texto,
    esc,
    fmt_compacto,
    fmt_dec,
    fmt_int,
    fmt_pct,
    renderizar,
    status,
)

DADOS_VAZIOS = {
    "avisos": [],
    "cargas": [],
    "execucoes": [],
    "duracao_ultimo_run": [],
    "erros": [],
    "resumo_erros": [],
}


def test_formatacao_pt_br():
    assert fmt_int(1204871) == "1.204.871"
    assert fmt_dec(1234.5) == "1.234,5"
    assert fmt_pct(0.0736) == "7,4%"
    assert fmt_compacto(1_204_871) == "1,2 mi"
    assert fmt_int(None) == "0" and fmt_int("abc") == "0"
    assert fmt_int("12.6") == "13"  # a API do SQL Warehouse devolve numeros como texto


def test_delta():
    assert delta_texto(110, 100, "20/09")[1] == "sobe"
    assert delta_texto(90, 100, "20/09")[1] == "desce"
    assert delta_texto(100, 100, "20/09")[1] == "neutro"
    assert delta_texto(1, None, None) == ("", "")


def test_status_sempre_tem_icone_e_texto():
    for valor in ["SUCESSO", "ERRO_SILVER", "ALERTA", "PENDENTE", "BLOQUEANTE"]:
        html = str(status(valor))
        assert valor in html and 'aria-hidden="true"' in html


def test_html_e_escapado():
    assert esc("<script>alert(1)</script>") == "&lt;script&gt;alert(1)&lt;/script&gt;"
    assert "<script>alert" not in barras([("<b>x</b>", 1, "<i>")])


def test_barras_sem_dados_e_com_valor_zero():
    assert "Sem dados" in barras([])
    assert 'style="width:0.0%"' in barras([("a", 0, ""), ("b", 5, "")])


def test_renderiza_pagina_sem_nenhuma_carga():
    html = renderizar(DADOS_VAZIOS, "dev", datetime(2026, 9, 20, tzinfo=timezone.utc))
    assert html.startswith("<!doctype html>")
    assert "Nenhuma carga concluída" in html
    assert 'lang="pt-BR"' in html and "prefers-color-scheme:dark" in html


def test_renderiza_avisos_de_consulta_indisponivel():
    dados = {**DADOS_VAZIOS, "avisos": ["kpi: tabela nao existe"]}
    assert "kpi: tabela nao existe" in renderizar(dados, "stg")
