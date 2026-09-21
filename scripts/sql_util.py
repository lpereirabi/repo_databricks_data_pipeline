"""Executa SQL em um SQL Warehouse via Databricks SDK. Usado pelo smoke test, pelo relatorio local e no terminal.

Autenticacao: a do SDK (perfil do `databricks auth login` ou DATABRICKS_HOST/DATABRICKS_TOKEN no CI).

    python scripts/sql_util.py "SELECT current_user()"
"""

from __future__ import annotations

import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def escolher_warehouse(w: WorkspaceClient) -> str:
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError("Nenhum SQL Warehouse encontrado no workspace")
    return warehouses[0].id


def executar(w: WorkspaceClient, warehouse_id: str, sql: str, timeout_s: int = 600) -> list[dict]:
    """Executa e devolve as linhas como dicts (valores como str/None, conforme a API)."""
    resp = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id, wait_timeout="30s"
    )
    inicio = time.time()
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() - inicio > timeout_s:
            raise TimeoutError(f"SQL excedeu {timeout_s}s: {sql[:120]}")
        time.sleep(3)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL falhou ({resp.status.state}): {resp.status.error}")
    if not resp.result or not resp.result.data_array:
        return []
    colunas = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(colunas, linha, strict=True)) for linha in resp.result.data_array]


def main() -> None:
    w = WorkspaceClient()
    wid = escolher_warehouse(w)
    for linha in executar(w, wid, " ".join(sys.argv[1:])):
        print(linha)


if __name__ == "__main__":
    main()
