"""Estagio de empacotamento (CI): garante que o wheel gerado e instalavel e expoe o entry point do job."""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

ESPERADO = [
    "mkt_pipeline/__init__.py",
    "mkt_pipeline/cli.py",
    "mkt_pipeline/pipeline.py",
    "mkt_pipeline/report.py",
]


def main() -> int:
    wheels = sorted(Path("dist").glob("*.whl"))
    if len(wheels) != 1:
        print(f"ERRO: esperava 1 wheel em dist/, achei {len(wheels)}: {[w.name for w in wheels]}")
        return 1
    with zipfile.ZipFile(wheels[0]) as z:
        nomes = z.namelist()
        faltando = [f for f in ESPERADO if f not in nomes]
        entry = next((n for n in nomes if n.endswith("entry_points.txt")), None)
        conteudo = z.read(entry).decode() if entry else ""
    if faltando or "mkt-pipeline = mkt_pipeline.cli:main" not in conteudo:
        print(f"ERRO: wheel invalido. arquivos faltando={faltando}; entry_points={conteudo!r}")
        return 1
    print(f"OK: {wheels[0].name} ({len(nomes)} arquivos) com entry point mkt-pipeline")
    return 0


if __name__ == "__main__":
    sys.exit(main())
