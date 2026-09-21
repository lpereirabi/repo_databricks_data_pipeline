"""Relatorio HTML (Fase 7): painel operacional das cargas + dashboard de negocio (Gold) + qualidade + governanca.

Uma unica pagina, autocontida (sem JS externo, sem CDN): abre offline e pode ser publicada em qualquer lugar.
`coletar` le as tabelas via uma funcao `fetch(sql) -> list[dict]` (Spark no job; SDK no script local) e
`renderizar` e uma funcao pura dados -> HTML (testavel sem Spark).
"""

from __future__ import annotations

import html as _html
import os
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .config import Settings
from .metadata import TABELAS

Fetch = Callable[[str], list[dict[str, Any]]]

ORDEM_ETAPAS = ["preparar", "bronze", "silver", "gold_dimensoes", "gold_fatos", "governanca", "relatorio"]
ORDEM_FAIXA_SALARIAL = ["Baixa", "Média", "Alta", "Muito Alta", "Não Informado"]
ORDEM_FAIXA_ETARIA = ["Até 39", "40-49", "50-59", "60-69", "70+", "Não Informado"]


# --------------------------------------------------------------------------------------------------
# Formatacao (pt-BR)
# --------------------------------------------------------------------------------------------------
def num(v: Any, padrao: float = 0.0) -> float:
    if v is None or v == "":
        return padrao
    if isinstance(v, (int, float, Decimal)):
        return float(v)
    try:
        return float(str(v))
    except ValueError:
        return padrao


def fmt_int(v: Any) -> str:
    return f"{round(num(v)):,}".replace(",", ".")


def fmt_dec(v: Any, casas: int = 1) -> str:
    return f"{num(v):,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(fracao: Any, casas: int = 1) -> str:
    return fmt_dec(num(fracao) * 100, casas) + "%"


def fmt_compacto(v: Any) -> str:
    n = num(v)
    for limite, sufixo in ((1e9, " bi"), (1e6, " mi"), (1e3, " mil")):
        if abs(n) >= limite:
            return fmt_dec(n / limite, 1) + sufixo
    return fmt_int(n)


def esc(v: Any) -> str:
    return _html.escape("" if v is None else str(v))


def dia(v: Any) -> str:
    return "" if v is None else str(v)[:10]


def ts(v: Any) -> str:
    return "" if v is None else str(v)[:19].replace("T", " ")


# --------------------------------------------------------------------------------------------------
# Coleta
# --------------------------------------------------------------------------------------------------
def coletar(fetch: Fetch, cfg: Settings) -> dict[str, Any]:
    t = cfg.tabela
    avisos: list[str] = []

    def q(nome: str, sql: str) -> list[dict[str, Any]]:
        try:
            return fetch(sql)
        except Exception as exc:  # noqa: BLE001 - um painel sem dados nao pode impedir o relatorio
            avisos.append(f"{nome}: {str(exc).splitlines()[0][:160]}")
            return []

    d: dict[str, Any] = {"avisos": avisos}
    d["cargas"] = q(
        "cargas",
        f"SELECT data_carga, arquivo, status, tentativas, linhas_bronze, linhas_silver, linhas_quarentena, "
        f"tamanho_bytes, atualizado_em, mensagem FROM {t('ctl', 'ctl_carga_arquivo')} ORDER BY data_carga DESC",
    )
    d["execucoes"] = q(
        "execucoes",
        f"SELECT run_id, etapa, data_carga, status, inicio_ts, duracao_seg, linhas_lidas, linhas_gravadas, "
        f"linhas_rejeitadas, mensagem FROM {t('ctl', 'log_execucao')} ORDER BY inicio_ts DESC LIMIT 40",
    )
    d["duracao_ultimo_run"] = q(
        "duracao",
        f"SELECT etapa, round(sum(duracao_seg), 1) seg FROM {t('ctl', 'log_execucao')} WHERE run_id = "
        f"(SELECT run_id FROM {t('ctl', 'log_execucao')} ORDER BY inicio_ts DESC LIMIT 1) GROUP BY etapa",
    )
    d["erros"] = q(
        "erros",
        f"SELECT erro_ts, severidade, etapa, tipo_erro, mensagem, data_carga FROM {t('ctl', 'log_erro')} "
        "ORDER BY erro_ts DESC LIMIT 15",
    )
    d["resumo_erros"] = q(
        "resumo_erros",
        f"SELECT severidade, count(*) n FROM {t('ctl', 'log_erro')} GROUP BY severidade",
    )

    sucesso = [c for c in d["cargas"] if c.get("status") == "SUCESSO"]
    ref = dia(sucesso[0]["data_carga"]) if sucesso else None
    ant = dia(sucesso[1]["data_carga"]) if len(sucesso) > 1 else None
    d["data_ref"], d["data_ant"] = ref, ant
    if not ref:
        return d

    sk = ref.replace("-", "")
    snap = t("gold", "fato_cliente_snapshot")
    filtro = f"WHERE f.sk_data_carga = {sk}"
    sks = f"{sk}, {ant.replace('-', '')}" if ant else sk
    d["kpi"] = q(
        "kpi",
        f"SELECT sk_data_carga, count(*) clientes, sum(total_gasto) gasto, avg(total_gasto) ticket, "
        f"avg(comprou) conversao, avg(qtd_campanhas_aceitas) campanhas, avg(salario_anual) salario, "
        f"avg(idade) idade FROM {snap} WHERE sk_data_carga IN ({sks}) GROUP BY sk_data_carga",
    )
    d["categoria"] = q(
        "categoria",
        f"SELECT c.categoria rotulo, sum(f.valor_gasto) valor FROM {t('gold', 'fato_gasto_categoria')} f "
        f"JOIN {t('gold', 'dim_categoria_produto')} c ON f.sk_categoria = c.sk_categoria {filtro} "
        "GROUP BY 1 ORDER BY 2 DESC",
    )
    d["canal"] = q(
        "canal",
        f"SELECT c.canal rotulo, sum(f.qtd_compras) valor FROM {t('gold', 'fato_compras_canal')} f "
        f"JOIN {t('gold', 'dim_canal')} c ON f.sk_canal = c.sk_canal {filtro} GROUP BY 1 ORDER BY 2 DESC",
    )
    d["campanha"] = q(
        "campanha",
        f"SELECT c.campanha rotulo, avg(f.aceitou) valor, sum(f.aceitou) n FROM "
        f"{t('gold', 'fato_resposta_campanha')} f JOIN {t('gold', 'dim_campanha')} c "
        f"ON f.sk_campanha = c.sk_campanha {filtro} GROUP BY 1 ORDER BY 1",
    )
    d["pais"] = q(
        "pais",
        f"SELECT p.pais rotulo, count(*) valor, sum(f.total_gasto) gasto FROM {snap} f "
        f"JOIN {t('gold', 'dim_pais')} p ON f.sk_pais = p.sk_pais {filtro} GROUP BY 1 ORDER BY 2 DESC",
    )
    d["faixa_salarial"] = q(
        "faixa_salarial",
        f"SELECT c.faixa_salarial rotulo, count(*) n, avg(f.total_gasto) valor FROM {snap} f "
        f"JOIN {t('gold', 'dim_cliente')} c ON f.sk_cliente = c.sk_cliente {filtro} GROUP BY 1",
    )
    d["faixa_etaria"] = q(
        "faixa_etaria",
        f"SELECT faixa_etaria rotulo, count(*) n, avg(total_gasto) valor FROM {snap} f {filtro} GROUP BY 1",
    )
    d["escolaridade"] = q(
        "escolaridade",
        f"SELECT e.escolaridade rotulo, e.nivel_ordem ordem, count(*) n, avg(f.total_gasto) valor FROM {snap} f "
        f"JOIN {t('gold', 'dim_escolaridade')} e ON f.sk_escolaridade = e.sk_escolaridade {filtro} "
        "GROUP BY 1, 2 ORDER BY 2",
    )
    d["dq"] = q(
        "dq",
        f"SELECT regra, descricao, severidade, acao, total, falhas, pct_falha FROM {t('ctl', 'dq_resultado')} "
        f"WHERE data_carga = DATE'{ref}' AND run_id = (SELECT run_id FROM {t('ctl', 'dq_resultado')} "
        f"WHERE data_carga = DATE'{ref}' ORDER BY avaliado_ts DESC LIMIT 1) ORDER BY severidade, regra",
    )
    d["scd"] = q(
        "scd",
        f"SELECT count(*) versoes, count(DISTINCT id_cliente) clientes, "
        f"sum(CASE WHEN flag_atual THEN 0 ELSE 1 END) historicas FROM {t('gold', 'dim_cliente')}",
    )
    uniao = " UNION ALL ".join(
        f"SELECT '{m.camada}.{m.nome}' tabela, '{m.camada}' camada, count(*) linhas FROM {t(m.camada, m.nome)}"
        for m in TABELAS
    )
    d["inventario"] = q("inventario", uniao)
    d["dicionario"] = q(
        "dicionario",
        f"SELECT camada, count(*) colunas, sum(CASE WHEN descricao <> '' THEN 1 ELSE 0 END) documentadas, "
        f"sum(CASE WHEN classificacao = 'confidencial' THEN 1 ELSE 0 END) confidenciais "
        f"FROM {t('ctl', 'dicionario_dados')} GROUP BY camada",
    )
    return d


# --------------------------------------------------------------------------------------------------
# Componentes HTML
# --------------------------------------------------------------------------------------------------
def barras(itens: list[tuple[str, float, str]], fmt: Callable[[float], str] = fmt_int) -> str:
    """Barras horizontais de UMA serie (magnitude). itens = (rotulo, valor, dica)."""
    if not itens:
        return '<p class="vazio">Sem dados.</p>'
    maximo = max((v for _, v, _ in itens), default=0) or 1
    linhas = []
    for rotulo, valor, dica in itens:
        largura = max(valor / maximo * 100, 0.6) if valor > 0 else 0
        linhas.append(
            f'<div class="bar-row" role="listitem" title="{esc(dica)}"><span class="bar-label">{esc(rotulo)}</span>'
            f'<span class="bar-track"><span class="bar-fill" style="width:{largura:.1f}%"></span></span>'
            f'<span class="bar-value">{esc(fmt(valor))}</span></div>'
        )
    return f'<div class="bars" role="list">{"".join(linhas)}</div>'


def tabela(cabecalhos: list[str], linhas: list[list[Any]], numericas: set[int] | None = None) -> str:
    numericas = numericas or set()
    if not linhas:
        return '<p class="vazio">Sem dados.</p>'
    th = "".join(
        f"<th{' class=num' if i in numericas else ''}>{esc(h)}</th>" for i, h in enumerate(cabecalhos)
    )
    corpo = "".join(
        "<tr>"
        + "".join(
            f"<td{' class=num' if i in numericas else ''}>{c if isinstance(c, Raw) else esc(c)}</td>"
            for i, c in enumerate(linha)
        )
        + "</tr>"
        for linha in linhas
    )
    return f'<div class="tabela-wrap"><table><thead><tr>{th}</tr></thead><tbody>{corpo}</tbody></table></div>'


class Raw(str):
    """Fragmento HTML ja seguro (nao escapar)."""


def status(valor: Any) -> Raw:
    """Status SEMPRE com icone + texto (a cor nunca carrega o significado sozinha)."""
    v = str(valor or "")
    if v in ("SUCESSO", "OK"):
        return Raw('<span class="pill bom"><span aria-hidden="true">✓</span> ' + esc(v) + "</span>")
    if v.startswith("ERRO") or v in ("BLOQUEANTE", "CRITICO"):
        return Raw('<span class="pill critico"><span aria-hidden="true">✕</span> ' + esc(v) + "</span>")
    if v in ("AVISO", "ALERTA", "PENDENTE") or v.endswith("_OK"):
        return Raw('<span class="pill atencao"><span aria-hidden="true">!</span> ' + esc(v) + "</span>")
    return Raw(f'<span class="pill neutro">{esc(v)}</span>')


def cartao(
    ident: str, titulo: str, subtitulo: str, corpo: str, tabela_html: str = "", larga: bool = False
) -> str:
    detalhes = (
        f'<details class="tv"><summary>Ver como tabela</summary>{tabela_html}</details>'
        if tabela_html
        else ""
    )
    classe = "card larga" if larga else "card"
    return (
        f'<section class="{classe}" id="{esc(ident)}"><h3>{esc(titulo)}</h3><p class="sub">{esc(subtitulo)}</p>'
        f"{corpo}{detalhes}</section>"
    )


def tile(rotulo: str, valor: str, delta: str = "", classe_delta: str = "") -> str:
    d = f'<span class="delta {classe_delta}">{esc(delta)}</span>' if delta else ""
    return f'<div class="tile"><span class="t-label">{esc(rotulo)}</span><span class="t-valor">{esc(valor)}</span>{d}</div>'


def delta_texto(
    atual: float, anterior: float | None, rotulo_ant: str | None, casas: int = 1
) -> tuple[str, str]:
    if anterior is None or not rotulo_ant:
        return "", ""
    if anterior == 0:
        return f"sem base em {rotulo_ant}", "neutro"
    var = (atual - anterior) / abs(anterior) * 100
    if abs(var) < 0.05:
        return f"= sem variação vs {rotulo_ant}", "neutro"
    seta = "▲" if var > 0 else "▼"
    return f"{seta} {fmt_dec(abs(var), casas)}% vs {rotulo_ant}", "sobe" if var > 0 else "desce"


def meter(fracao: float) -> Raw:
    pct = min(max(fracao, 0), 1) * 100
    return Raw(
        f'<span class="meter" title="{fmt_dec(pct, 2)}%"><span style="width:{pct:.1f}%"></span></span>'
    )


def _ordenar(linhas: list[dict[str, Any]], ordem: list[str]) -> list[dict[str, Any]]:
    idx = {k: i for i, k in enumerate(ordem)}
    return sorted(linhas, key=lambda r: idx.get(r.get("rotulo"), 99))


# --------------------------------------------------------------------------------------------------
# Renderizacao
# --------------------------------------------------------------------------------------------------
def _secao_operacao(d: dict[str, Any]) -> str:
    cargas, execs = d["cargas"], d["execucoes"]
    ok = sum(1 for c in cargas if c.get("status") == "SUCESSO")
    erro = sum(1 for c in cargas if str(c.get("status", "")).startswith("ERRO"))
    ultimo = execs[0] if execs else {}
    run_atual = ultimo.get("run_id")
    exec_run = [e for e in execs if e.get("run_id") == run_atual]
    run_ok = bool(exec_run) and all(e.get("status") == "SUCESSO" for e in exec_run)
    duracao_total = sum(num(x.get("seg")) for x in d["duracao_ultimo_run"])
    erros = {r["severidade"]: int(num(r["n"])) for r in d["resumo_erros"]}
    ult_carga = next((c for c in cargas if c.get("status") == "SUCESSO"), None)
    pct_q = (
        num(ult_carga["linhas_quarentena"]) / num(ult_carga["linhas_bronze"])
        if ult_carga and num(ult_carga["linhas_bronze"])
        else 0
    )

    tiles = "".join(
        [
            tile(
                "Última execução",
                "Sucesso" if run_ok else ("Com falha" if exec_run else "—"),
                ts(ultimo.get("inicio_ts")),
            ),
            tile("Cargas concluídas", fmt_int(ok), f"{erro} com erro" if erro else "nenhuma com erro"),
            tile(
                "Linhas na última carga",
                fmt_int(ult_carga["linhas_bronze"]) if ult_carga else "—",
                dia(ult_carga["data_carga"]) if ult_carga else "",
            ),
            tile("Em quarentena", fmt_pct(pct_q, 2), "da última carga"),
            tile(
                "Duração da última execução",
                f"{fmt_dec(duracao_total / 60, 1)} min" if duracao_total else "—",
            ),
            tile(
                "Erros / avisos registrados",
                f"{fmt_int(erros.get('ERRO', 0))} / {fmt_int(erros.get('AVISO', 0))}",
            ),
        ]
    )

    linhas_cargas = [
        [
            dia(c["data_carga"]),
            c.get("arquivo"),
            status(c.get("status")),
            fmt_int(c.get("linhas_bronze")),
            fmt_int(c.get("linhas_silver")),
            fmt_int(c.get("linhas_quarentena")),
            fmt_int(c.get("tentativas")),
            ts(c.get("atualizado_em")),
        ]
        for c in cargas
    ]
    cartao_cargas = cartao(
        "cargas",
        "Cargas por arquivo",
        "Uma linha por marketing_AAAAMMDD.csv, com o estado na máquina de estados do pipeline",
        tabela(
            [
                "Data da carga",
                "Arquivo",
                "Status",
                "Bronze",
                "Silver",
                "Quarentena",
                "Tentativas",
                "Atualizado",
            ],
            linhas_cargas,
            {3, 4, 5, 6},
        ),
        larga=True,
    )

    ordem = {e: i for i, e in enumerate(ORDEM_ETAPAS)}
    dur = sorted(d["duracao_ultimo_run"], key=lambda r: ordem.get(r["etapa"], 99))
    dur_itens = [(r["etapa"], num(r["seg"]), f"{r['etapa']}: {fmt_dec(r['seg'])} s") for r in dur]
    cartao_dur = cartao(
        "duracao",
        "Duração por etapa",
        "Última execução, em segundos",
        barras(dur_itens, lambda v: f"{fmt_dec(v)} s"),
        tabela(["Etapa", "Segundos"], [[r["etapa"], fmt_dec(r["seg"])] for r in dur], {1}),
    )

    linhas_exec = [
        [
            ts(e.get("inicio_ts")),
            e.get("etapa"),
            dia(e.get("data_carga")),
            status(e.get("status")),
            fmt_dec(e.get("duracao_seg")),
            fmt_int(e.get("linhas_lidas")) if e.get("linhas_lidas") is not None else "",
            fmt_int(e.get("linhas_gravadas")) if e.get("linhas_gravadas") is not None else "",
            fmt_int(e.get("linhas_rejeitadas")) if e.get("linhas_rejeitadas") is not None else "",
            e.get("mensagem") or "",
        ]
        for e in execs[:25]
    ]
    cartao_exec = cartao(
        "execucoes",
        "Log de integração",
        "Últimas execuções por etapa (tabela ctl.log_execucao)",
        tabela(
            ["Início", "Etapa", "Carga", "Status", "Seg", "Lidas", "Gravadas", "Rejeitadas", "Mensagem"],
            linhas_exec,
            {4, 5, 6, 7},
        ),
        larga=True,
    )
    linhas_erro = [
        [
            ts(e.get("erro_ts")),
            status(e.get("severidade")),
            e.get("etapa"),
            e.get("tipo_erro"),
            dia(e.get("data_carga")),
            (e.get("mensagem") or "")[:200],
        ]
        for e in d["erros"]
    ]
    cartao_erros = cartao(
        "erros",
        "Erros e avisos",
        "Últimos registros de ctl.log_erro (stacktrace completo na tabela)",
        tabela(["Quando", "Severidade", "Etapa", "Tipo", "Carga", "Mensagem"], linhas_erro)
        if linhas_erro
        else '<p class="vazio">Nenhum erro ou aviso registrado. ✓</p>',
        larga=True,
    )
    return f'<div class="tiles">{tiles}</div><div class="grid">{cartao_cargas}{cartao_dur}{cartao_exec}{cartao_erros}</div>'


def _secao_negocio(d: dict[str, Any]) -> str:
    if not d.get("data_ref"):
        return '<p class="vazio">Nenhuma carga concluída ainda.</p>'
    ref, ant = d["data_ref"], d.get("data_ant")
    sk_ref, sk_ant = int(ref.replace("-", "")), int(ant.replace("-", "")) if ant else None
    kpi = {int(num(k["sk_data_carga"])): k for k in d["kpi"]}
    a, b = kpi.get(sk_ref, {}), kpi.get(sk_ant, {}) if sk_ant else {}

    def dl(campo: str, casas: int = 1) -> tuple[str, str]:
        return delta_texto(num(a.get(campo)), num(b.get(campo)) if b else None, ant, casas)

    dg, cg = dl("gasto")
    hero = (
        f'<div class="hero"><span class="t-label">Gasto total dos clientes · carga {esc(ref)}</span>'
        f'<span class="hero-valor">{esc(fmt_compacto(a.get("gasto")))}</span>'
        f'<span class="delta {cg}">{esc(dg) or "primeira carga concluída"}</span></div>'
    )
    tiles = "".join(
        [
            tile("Clientes", fmt_int(a.get("clientes")), *dl("clientes")),
            tile("Gasto médio por cliente", fmt_dec(a.get("ticket"), 1), *dl("ticket")),
            tile("Taxa de conversão", fmt_pct(a.get("conversao")), *dl("conversao")),
            tile("Campanhas aceitas (média)", fmt_dec(a.get("campanhas"), 2), *dl("campanhas", 1)),
            tile("Salário anual médio", fmt_int(a.get("salario")), *dl("salario")),
            tile("Idade média", fmt_dec(a.get("idade"), 1), *dl("idade")),
        ]
    )

    def itens(
        chave: str, fmt_dica: Callable[[dict[str, Any]], str], campo: str = "valor"
    ) -> list[tuple[str, float, str]]:
        return [(r["rotulo"], num(r[campo]), fmt_dica(r)) for r in d.get(chave, [])]

    total_cat = sum(num(r["valor"]) for r in d.get("categoria", [])) or 1
    cat = cartao(
        "categoria",
        "Gasto por categoria",
        "Soma do gasto de todos os clientes, da maior para a menor",
        barras(
            itens(
                "categoria",
                lambda r: f"{r['rotulo']}: {fmt_int(r['valor'])} ({fmt_pct(num(r['valor']) / total_cat)})",
            )
        ),
        tabela(
            ["Categoria", "Gasto", "% do total"],
            [
                [r["rotulo"], fmt_int(r["valor"]), fmt_pct(num(r["valor"]) / total_cat)]
                for r in d.get("categoria", [])
            ],
            {1, 2},
        ),
    )
    total_canal = sum(num(r["valor"]) for r in d.get("canal", [])) or 1
    canal = cartao(
        "canal",
        "Compras por canal",
        "Quantidade de compras em cada canal",
        barras(itens("canal", lambda r: f"{r['rotulo']}: {fmt_int(r['valor'])} compras")),
        tabela(
            ["Canal", "Compras", "% do total"],
            [
                [r["rotulo"], fmt_int(r["valor"]), fmt_pct(num(r["valor"]) / total_canal)]
                for r in d.get("canal", [])
            ],
            {1, 2},
        ),
    )
    camp = cartao(
        "campanha",
        "Aceitação por campanha",
        "% de clientes que compraram na campanha",
        barras(
            itens(
                "campanha", lambda r: f"{r['rotulo']}: {fmt_int(r['n'])} clientes ({fmt_pct(r['valor'], 2)})"
            ),
            lambda v: fmt_pct(v, 1),
        ),
        tabela(
            ["Campanha", "Clientes que aceitaram", "Taxa"],
            [[r["rotulo"], fmt_int(r["n"]), fmt_pct(r["valor"], 2)] for r in d.get("campanha", [])],
            {1, 2},
        ),
    )
    pais = cartao(
        "pais",
        "Clientes por país",
        "Quantidade de clientes na carga",
        barras(
            itens(
                "pais",
                lambda r: f"{r['rotulo']}: {fmt_int(r['valor'])} clientes · gasto {fmt_int(r['gasto'])}",
            )
        ),
        tabela(
            ["País", "Clientes", "Gasto total"],
            [[r["rotulo"], fmt_int(r["valor"]), fmt_int(r["gasto"])] for r in d.get("pais", [])],
            {1, 2},
        ),
    )
    fs = _ordenar(d.get("faixa_salarial", []), ORDEM_FAIXA_SALARIAL)
    faixa_sal = cartao(
        "faixa-salarial",
        "Gasto médio por faixa salarial",
        "Média do gasto por cliente; faixas definidas em ctl_parametro",
        barras(
            [
                (
                    r["rotulo"],
                    num(r["valor"]),
                    f"{r['rotulo']}: {fmt_dec(r['valor'])} médio · {fmt_int(r['n'])} clientes",
                )
                for r in fs
            ],
            lambda v: fmt_dec(v, 0),
        ),
        tabela(
            ["Faixa", "Clientes", "Gasto médio"],
            [[r["rotulo"], fmt_int(r["n"]), fmt_dec(r["valor"])] for r in fs],
            {1, 2},
        ),
    )
    fe = _ordenar(d.get("faixa_etaria", []), ORDEM_FAIXA_ETARIA)
    faixa_eta = cartao(
        "faixa-etaria",
        "Clientes por faixa etária",
        "Idade na data da carga",
        barras(
            [
                (
                    r["rotulo"],
                    num(r["n"]),
                    f"{r['rotulo']}: {fmt_int(r['n'])} clientes · gasto médio {fmt_dec(r['valor'])}",
                )
                for r in fe
            ]
        ),
        tabela(
            ["Faixa", "Clientes", "Gasto médio"],
            [[r["rotulo"], fmt_int(r["n"]), fmt_dec(r["valor"])] for r in fe],
            {1, 2},
        ),
    )
    esc_ = d.get("escolaridade", [])
    escol = cartao(
        "escolaridade",
        "Gasto médio por escolaridade",
        "Do menor para o maior grau",
        barras(
            [
                (
                    r["rotulo"],
                    num(r["valor"]),
                    f"{r['rotulo']}: {fmt_dec(r['valor'])} médio · {fmt_int(r['n'])} clientes",
                )
                for r in esc_
            ],
            lambda v: fmt_dec(v, 0),
        ),
        tabela(
            ["Escolaridade", "Clientes", "Gasto médio"],
            [[r["rotulo"], fmt_int(r["n"]), fmt_dec(r["valor"])] for r in esc_],
            {1, 2},
        ),
    )
    return f'{hero}<div class="tiles">{tiles}</div><div class="grid">{cat}{canal}{camp}{pais}{faixa_sal}{faixa_eta}{escol}</div>'


def _secao_qualidade(d: dict[str, Any]) -> str:
    dq = d.get("dq", [])
    if not dq:
        return '<p class="vazio">Sem resultados de qualidade.</p>'
    linhas = [
        [
            r["regra"],
            r["descricao"],
            status(r["severidade"] if int(num(r["falhas"])) else "OK"),
            r["acao"],
            fmt_int(r["falhas"]),
            meter(num(r["pct_falha"]) / 100),
            fmt_dec(r["pct_falha"], 2) + "%",
        ]
        for r in dq
    ]
    bloq = sum(int(num(r["falhas"])) for r in dq if r["severidade"] == "BLOQUEANTE")
    alertas = sum(1 for r in dq if r["severidade"] == "ALERTA" and int(num(r["falhas"])))
    n_regras_alerta = sum(1 for r in dq if r["severidade"] == "ALERTA")
    resumo_tiles = "".join(
        [
            tile("Linhas avaliadas", fmt_int(dq[0]["total"]), "carga " + str(d["data_ref"])),
            tile("Violações bloqueantes", fmt_int(bloq), "linhas → quarentena"),
            tile("Regras de alerta acionadas", fmt_int(alertas), "de " + str(n_regras_alerta)),
        ]
    )
    resumo = '<div class="tiles">' + resumo_tiles + "</div>"
    cart = cartao(
        "dq",
        "Regras de qualidade de dados",
        "BLOQUEANTE = linha vai para a quarentena · ALERTA = linha segue sinalizada em dq_alertas",
        tabela(["Regra", "Descrição", "Estado", "Ação", "Falhas", "", "%"], linhas, {4, 6}),
        larga=True,
    )
    return f'{resumo}<div class="grid">{cart}</div>'


def _secao_governanca(d: dict[str, Any]) -> str:
    inv = d.get("inventario", [])
    dic = d.get("dicionario", [])
    scd = (d.get("scd") or [{}])[0]
    doc = sum(num(r["documentadas"]) for r in dic)
    col = sum(num(r["colunas"]) for r in dic)
    conf = sum(num(r["confidenciais"]) for r in dic)
    tiles = "".join(
        [
            tile("Tabelas Delta", fmt_int(len(inv))),
            tile(
                "Colunas no dicionário", fmt_int(col), f"{fmt_pct(doc / col) if col else '0%'} com descrição"
            ),
            tile("Colunas confidenciais", fmt_int(conf), "id, nascimento, salário, idade"),
            tile(
                "Versões de cliente (SCD2)",
                fmt_int(scd.get("versoes")),
                f"{fmt_int(scd.get('historicas'))} históricas · {fmt_int(scd.get('clientes'))} clientes",
            ),
        ]
    )
    ordem_camada = {"bronze": 0, "silver": 1, "gold": 2, "ctl": 3}
    linhas = [
        [r["tabela"], fmt_int(r["linhas"])]
        for r in sorted(inv, key=lambda r: (ordem_camada.get(r["camada"], 9), r["tabela"]))
    ]
    cart = cartao(
        "inventario",
        "Inventário de tabelas",
        "Linhas por tabela em cada camada do Medalhão",
        tabela(["Tabela (camada.nome)", "Linhas"], linhas, {1}),
    )
    return f'<div class="tiles">{tiles}</div><div class="grid">{cart}</div>'


CSS = """
:root{color-scheme:light;--page:#f9f9f7;--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;
--grid:#e1e0d9;--axis:#c3c2b7;--border:rgba(11,11,11,.10);--series:#2a78d6;--series-soft:#cde2fb;
--good:#0ca30c;--good-ink:#006300;--warn:#fab219;--crit:#d03b3b;}
@media (prefers-color-scheme:dark){:root:where(:not([data-theme="light"])){color-scheme:dark;--page:#0d0d0d;--surface:#1a1a19;
--ink:#fff;--ink2:#c3c2b7;--muted:#898781;--grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);
--series:#3987e5;--series-soft:#184f95;--good-ink:#0ca30c;}}
:root[data-theme="dark"]{color-scheme:dark;--page:#0d0d0d;--surface:#1a1a19;--ink:#fff;--ink2:#c3c2b7;--muted:#898781;
--grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);--series:#3987e5;--series-soft:#184f95;--good-ink:#0ca30c;}
*{box-sizing:border-box}
body{margin:0;background:var(--page);color:var(--ink);font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
header{background:var(--surface);border-bottom:1px solid var(--border)}
.wrap{max-width:1180px;margin:0 auto;padding:0 16px}
.topo{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;padding:20px 0 12px}
h1{font-size:22px;margin:0;font-weight:650}
.meta{color:var(--ink2);font-size:13px}
.badge{display:inline-block;border:1px solid var(--border);border-radius:999px;padding:1px 10px;font-size:12px;margin-left:8px;color:var(--ink2)}
button#tema{background:transparent;color:var(--ink);border:1px solid var(--axis);border-radius:8px;padding:6px 12px;cursor:pointer;font:inherit}
nav{display:flex;flex-wrap:wrap;gap:4px;padding-bottom:10px}
nav a{color:var(--ink2);text-decoration:none;padding:6px 12px;border-radius:8px;font-size:14px}
nav a:hover{background:var(--grid);color:var(--ink)}
main{padding:8px 0 48px}
h2{font-size:18px;margin:32px 0 4px;font-weight:650}
.lead{color:var(--ink2);margin:0 0 14px;font-size:14px}
.hero{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px 24px;margin-bottom:12px;display:flex;flex-direction:column;gap:2px}
.hero-valor{font-size:56px;line-height:1.1;font-weight:650;letter-spacing:-.02em}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:12px}
.tile{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px 16px;display:flex;flex-direction:column;gap:2px;min-width:0}
.t-label{color:var(--ink2);font-size:13px}
.t-valor{font-size:26px;font-weight:650;line-height:1.2;overflow-wrap:anywhere}
.delta{font-size:12.5px;color:var(--ink2)}
.delta.sobe{color:var(--good-ink)}.delta.desce{color:var(--crit)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:12px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px 18px;min-width:0}
.card.larga{grid-column:1/-1}
.card h3{margin:0;font-size:15px;font-weight:650}
.sub{margin:2px 0 12px;color:var(--ink2);font-size:13px}
.bars{display:flex;flex-direction:column;gap:6px}
.bar-row{display:grid;grid-template-columns:minmax(84px,150px) 1fr auto;gap:10px;align-items:center;padding:3px 4px;border-radius:6px;min-height:28px}
.bar-row:hover{background:var(--grid)}
.bar-label{font-size:13.5px;color:var(--ink);overflow-wrap:anywhere}
.bar-track{height:20px;display:block;border-left:1px solid var(--axis)}
.bar-fill{display:block;height:20px;max-width:100%;background:var(--series);border-radius:0 4px 4px 0}
.bar-value{font-size:13px;color:var(--ink2);font-variant-numeric:tabular-nums;min-width:56px;text-align:right}
.tabela-wrap{overflow-x:auto;margin-top:6px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--grid);vertical-align:top}
th{color:var(--ink2);font-weight:600;white-space:nowrap}
td{white-space:nowrap}td:last-child{white-space:normal}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
.pill{display:inline-flex;gap:5px;align-items:center;font-size:12px;border:1px solid var(--border);border-radius:999px;padding:1px 9px;white-space:nowrap}
.pill span[aria-hidden]{font-weight:700}
.pill.bom span[aria-hidden]{color:var(--good)}.pill.critico span[aria-hidden]{color:var(--crit)}.pill.atencao span[aria-hidden]{color:#c98500}
.meter{display:block;width:90px;height:8px;background:var(--series-soft);border-radius:4px;overflow:hidden}
.meter span{display:block;height:8px;background:var(--series)}
details.tv{margin-top:12px}
details.tv summary{cursor:pointer;color:var(--ink2);font-size:13px}
.vazio{color:var(--ink2);font-size:14px}
footer{color:var(--muted);font-size:12.5px;padding:8px 0 40px}
@media (max-width:560px){.hero-valor{font-size:44px}.grid{grid-template-columns:1fr}}
@media print{button#tema,nav{display:none}.card,.tile,.hero{break-inside:avoid}}
"""

JS = """
(function(){var b=document.getElementById('tema'),r=document.documentElement;
function set(t){r.setAttribute('data-theme',t);try{localStorage.setItem('tema',t)}catch(e){}}
try{var s=localStorage.getItem('tema');if(s)r.setAttribute('data-theme',s)}catch(e){}
b.addEventListener('click',function(){var d=r.getAttribute('data-theme')||(matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light');set(d==='dark'?'light':'dark')})})();
"""


def renderizar(d: dict[str, Any], ambiente: str, gerado_em: datetime | None = None) -> str:
    gerado_em = gerado_em or datetime.now(timezone.utc)
    ref = d.get("data_ref")
    avisos = "".join(f"<li>{esc(a)}</li>" for a in d.get("avisos", []))
    rodape_avisos = f"<p>Consultas indisponíveis nesta geração:</p><ul>{avisos}</ul>" if avisos else ""
    return f"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Marketing Pipeline</title>
<style>{CSS}</style>
</head>
<body>
<header><div class="wrap">
  <div class="topo">
    <div><h1>Pipeline de Marketing<span class="badge">{esc(ambiente)}</span></h1>
    <div class="meta">Última carga concluída: {esc(ref or "nenhuma")} · gerado em {esc(gerado_em.strftime("%Y-%m-%d %H:%M"))} UTC</div></div>
    <button id="tema" type="button" aria-label="Alternar tema claro/escuro">Tema</button>
  </div>
  <nav aria-label="Seções"><a href="#negocio">Negócio</a><a href="#operacao">Operação das cargas</a><a href="#qualidade">Qualidade</a><a href="#governanca">Governança</a></nav>
</div></header>
<main class="wrap">
  <h2 id="negocio">Negócio</h2>
  <p class="lead">Indicadores da camada Gold na última carga concluída, com variação frente à carga anterior.</p>
  {_secao_negocio(d)}
  <h2 id="operacao">Operação das cargas</h2>
  <p class="lead">Estado dos arquivos, execuções, duração e erros (tabelas de controle e log).</p>
  {_secao_operacao(d)}
  <h2 id="qualidade">Qualidade de dados</h2>
  <p class="lead">Resultado das regras aplicadas na Silver para a última carga concluída.</p>
  {_secao_qualidade(d)}
  <h2 id="governanca">Governança</h2>
  <p class="lead">Inventário do Medalhão, dicionário de dados e histórico do cliente (SCD Tipo 2).</p>
  {_secao_governanca(d)}
</main>
<footer><div class="wrap">Gerado automaticamente pelo job de marketing (etapa relatorio). Valores na moeda de origem.{rodape_avisos}</div></footer>
<script>{JS}</script>
</body>
</html>
"""


# --------------------------------------------------------------------------------------------------
# Saida
# --------------------------------------------------------------------------------------------------
def gerar_relatorio(spark, cfg: Settings) -> str:  # noqa: ANN001 - SparkSession (import tardio no runtime)
    """Le o Lakehouse via Spark, renderiza e grava no volume `relatorios` (latest + copia por data)."""

    def fetch(sql: str) -> list[dict[str, Any]]:
        return [r.asDict() for r in spark.sql(sql).collect()]

    dados = coletar(fetch, cfg)
    pagina = renderizar(dados, cfg.ambiente)
    pasta = cfg.relatorios_path
    os.makedirs(pasta, exist_ok=True)
    latest = os.path.join(pasta, "dashboard_latest.html")
    for caminho in (
        latest,
        os.path.join(pasta, f"dashboard_{(dados.get('data_ref') or 'sem_carga').replace('-', '')}.html"),
    ):
        with open(caminho, "w", encoding="utf-8") as f:
            f.write(pagina)
    return latest
