"""Smoke test de dados apos uma execucao do pipeline: reconciliacao entre camadas e integridade do modelo.

    python scripts/verificar_carga.py --catalog workspace --prefixo mkt_stg --data-carga 2026-09-20

Sai com codigo 1 se qualquer verificacao falhar (o estagio STG do CI usa isso como gate para liberar PROD).
"""

from __future__ import annotations

import argparse
import sys

from databricks.sdk import WorkspaceClient

from sql_util import escolher_warehouse, executar


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--prefixo", required=True)
    p.add_argument("--data-carga", required=True, help="AAAA-MM-DD")
    a = p.parse_args()

    w = WorkspaceClient()
    wid = escolher_warehouse(w)
    cat, pre, d = a.catalog, a.prefixo, a.data_carga
    sk_carga = d.replace("-", "")

    def t(camada: str, nome: str) -> str:
        return f"{cat}.{pre}_{camada}.{nome}"

    def um(sql: str) -> str | None:
        linhas = executar(w, wid, sql)
        return next(iter(linhas[0].values())) if linhas else None

    def n(sql: str) -> int:
        return int(um(sql) or 0)

    bronze = n(f"SELECT count(*) FROM {t('bronze', 'marketing_raw')} WHERE _data_carga = DATE'{d}'")
    silver = n(f"SELECT count(*) FROM {t('silver', 'marketing_cliente')} WHERE _data_carga = DATE'{d}'")
    quarent = n(f"SELECT count(*) FROM {t('silver', 'marketing_quarentena')} WHERE _data_carga = DATE'{d}'")
    snap = n(f"SELECT count(*) FROM {t('gold', 'fato_cliente_snapshot')} WHERE sk_data_carga = {sk_carga}")
    gasto = n(f"SELECT count(*) FROM {t('gold', 'fato_gasto_categoria')} WHERE sk_data_carga = {sk_carga}")
    canal = n(f"SELECT count(*) FROM {t('gold', 'fato_compras_canal')} WHERE sk_data_carga = {sk_carga}")
    camp = n(f"SELECT count(*) FROM {t('gold', 'fato_resposta_campanha')} WHERE sk_data_carga = {sk_carga}")

    soma_snap = um(
        f"SELECT sum(total_gasto) FROM {t('gold', 'fato_cliente_snapshot')} WHERE sk_data_carga = {sk_carga}"
    )
    soma_fato = um(
        f"SELECT sum(valor_gasto) FROM {t('gold', 'fato_gasto_categoria')} WHERE sk_data_carga = {sk_carga}"
    )

    checks: list[tuple[str, bool, str]] = [
        (
            "controle: carga com status SUCESSO",
            um(f"SELECT status FROM {t('ctl', 'ctl_carga_arquivo')} WHERE data_carga = DATE'{d}'")
            == "SUCESSO",
            "",
        ),
        ("bronze > 0", bronze > 0, f"bronze={bronze}"),
        ("bronze = silver + quarentena", bronze == silver + quarent, f"{bronze} vs {silver}+{quarent}"),
        ("silver = fato_cliente_snapshot", silver == snap, f"{silver} vs {snap}"),
        ("fato_gasto_categoria = 6 x snapshot", gasto == 6 * snap, f"{gasto} vs {6 * snap}"),
        ("fato_compras_canal = 3 x snapshot", canal == 3 * snap, f"{canal} vs {3 * snap}"),
        ("fato_resposta_campanha = 5 x snapshot", camp == 5 * snap, f"{camp} vs {5 * snap}"),
        (
            "soma de gasto: snapshot = fato por categoria",
            soma_snap == soma_fato,
            f"{soma_snap} vs {soma_fato}",
        ),
        (
            "dim_cliente: 1 versao atual por cliente",
            n(
                f"SELECT count(*) FROM (SELECT id_cliente FROM {t('gold', 'dim_cliente')} "
                "WHERE flag_atual GROUP BY id_cliente HAVING count(*) > 1)"
            )
            == 0,
            "",
        ),
        (
            "dim_cliente: vigencia valida (inicio <= fim)",
            n(f"SELECT count(*) FROM {t('gold', 'dim_cliente')} WHERE inicio_vigencia > fim_vigencia") == 0,
            "",
        ),
        (
            "fato sem chave orfa (cliente/pais/escolaridade/estado civil)",
            n(
                f"SELECT count(*) FROM {t('gold', 'fato_cliente_snapshot')} f "
                f"LEFT JOIN {t('gold', 'dim_cliente')} c ON f.sk_cliente = c.sk_cliente "
                f"LEFT JOIN {t('gold', 'dim_pais')} p ON f.sk_pais = p.sk_pais "
                f"LEFT JOIN {t('gold', 'dim_escolaridade')} e ON f.sk_escolaridade = e.sk_escolaridade "
                f"LEFT JOIN {t('gold', 'dim_estado_civil')} s ON f.sk_estado_civil = s.sk_estado_civil "
                f"WHERE f.sk_data_carga = {sk_carga} AND (c.sk_cliente IS NULL OR p.sk_pais IS NULL "
                "OR e.sk_escolaridade IS NULL OR s.sk_estado_civil IS NULL)"
            )
            == 0,
            "",
        ),
        (
            "fato sem data orfa (carga e cadastro)",
            n(
                f"SELECT count(*) FROM {t('gold', 'fato_cliente_snapshot')} f "
                f"LEFT JOIN {t('gold', 'dim_data')} d1 ON f.sk_data_carga = d1.sk_data "
                f"LEFT JOIN {t('gold', 'dim_data')} d2 ON f.sk_data_cadastro = d2.sk_data "
                f"WHERE f.sk_data_carga = {sk_carga} AND (d1.sk_data IS NULL OR d2.sk_data IS NULL)"
            )
            == 0,
            "",
        ),
        (
            "log_execucao sem etapa com ERRO nesta carga",
            n(
                f"SELECT count(*) FROM {t('ctl', 'log_execucao')} "
                f"WHERE data_carga = DATE'{d}' AND status = 'ERRO' AND run_id = "
                f"(SELECT run_id_ultimo FROM {t('ctl', 'ctl_carga_arquivo')} WHERE data_carga = DATE'{d}')"
            )
            == 0,
            "",
        ),
    ]

    falhas = 0
    for nome, ok, detalhe in checks:
        print(f"[{'OK ' if ok else 'FALHA'}] {nome} {detalhe}")
        falhas += 0 if ok else 1
    print(f"\n{len(checks) - falhas}/{len(checks)} verificacoes ok")
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())
