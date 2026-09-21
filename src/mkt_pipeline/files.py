"""Descoberta e validacao de arquivos na landing. Python puro (sem Spark) para ser facil de testar.

Convencao de nome: ``<prefixo>_AAAAMMDD.<extensao>`` (ex.: ``marketing_20260920.csv``).
A data no nome e o **parametro de carga** (`data_carga`).
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


@dataclass(frozen=True)
class ArquivoLanding:
    data_carga: date
    nome: str
    caminho: str
    tamanho_bytes: int


def nome_esperado(prefixo: str, data_carga: date, extensao: str) -> str:
    return f"{prefixo}_{data_carga:%Y%m%d}.{extensao}"


def extrair_data(nome: str, prefixo: str, extensao: str) -> date | None:
    """Devolve a data do nome do arquivo ou None se o nome nao segue a convencao (ou a data e invalida)."""
    padrao = re.compile(rf"^{re.escape(prefixo)}_(\d{{8}})\.{re.escape(extensao)}$", re.IGNORECASE)
    m = padrao.match(nome)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def listar_arquivos(pasta: str, prefixo: str, extensao: str) -> tuple[list[ArquivoLanding], list[str]]:
    """Lista a landing. Retorna (validos ordenados por data, nomes com formato invalido)."""
    validos: list[ArquivoLanding] = []
    invalidos: list[str] = []
    if not os.path.isdir(pasta):
        return validos, invalidos
    for entrada in sorted(os.listdir(pasta)):
        caminho = os.path.join(pasta, entrada)
        if not os.path.isfile(caminho) or not entrada.lower().endswith(f".{extensao.lower()}"):
            continue
        data = extrair_data(entrada, prefixo, extensao)
        if data is None:
            invalidos.append(entrada)
            continue
        validos.append(ArquivoLanding(data, entrada, caminho, os.path.getsize(caminho)))
    validos.sort(key=lambda a: a.data_carga)
    return validos, invalidos


def sha256_arquivo(caminho: str | Path, bloco: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        while chunk := f.read(bloco):
            h.update(chunk)
    return h.hexdigest()
