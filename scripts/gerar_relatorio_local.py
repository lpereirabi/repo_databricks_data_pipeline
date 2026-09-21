"""Gera a pagina HTML localmente lendo as tabelas via SQL Warehouse (sem precisar rodar o job).

python scripts/gerar_relatorio_local.py --catalog workspace --prefixo mkt_dev --ambiente dev --saida out/relatorio.html
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from databricks.sdk import WorkspaceClient  # noqa: E402

from mkt_pipeline.config import criar_settings  # noqa: E402
from mkt_pipeline.report import coletar, renderizar  # noqa: E402
from sql_util import escolher_warehouse, executar  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--prefixo", required=True)
    p.add_argument("--ambiente", default="dev")
    p.add_argument("--saida", default="out/relatorio.html")
    a = p.parse_args()

    w = WorkspaceClient()
    wid = escolher_warehouse(w)
    cfg = criar_settings(a.ambiente, a.catalog, a.prefixo)
    dados = coletar(lambda sql: executar(w, wid, sql), cfg)
    destino = Path(a.saida)
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(renderizar(dados, a.ambiente), encoding="utf-8")
    print(f"relatorio gravado em {destino.resolve()}")
    for aviso in dados["avisos"]:
        print("AVISO:", aviso)


if __name__ == "__main__":
    main()
