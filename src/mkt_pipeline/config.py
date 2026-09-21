"""Configuracao de ambiente: onde (catalogo/schemas) e como (run_id) o pipeline executa.

Os nomes de objetos sao montados aqui e em mais nenhum lugar. Cada ambiente (dev/stg/prod) usa o mesmo
catalogo e um prefixo de schema proprio (`mkt_stg_bronze`, `mkt_prod_bronze`, ...). No Free Edition nao
e possivel criar catalogos via API; com um workspace pago basta trocar `catalog` (variavel do bundle).
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass

CAMADAS = ("landing", "bronze", "silver", "gold", "ctl")

_IDENTIFICADOR = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validar_identificador(valor: str, campo: str) -> str:
    if not _IDENTIFICADOR.match(valor):
        raise ValueError(f"{campo} invalido: {valor!r} (use apenas letras, numeros e underscore)")
    return valor


@dataclass(frozen=True)
class Settings:
    ambiente: str
    catalog: str
    prefixo: str
    run_id: str

    def __post_init__(self) -> None:
        # Os nomes entram em comandos SQL: valida para impedir injecao via parametro de job.
        _validar_identificador(self.catalog, "catalog")
        _validar_identificador(self.prefixo, "prefixo")
        _validar_identificador(self.ambiente, "ambiente")

    def schema(self, camada: str) -> str:
        if camada not in CAMADAS:
            raise ValueError(f"camada desconhecida: {camada!r}")
        return f"{self.prefixo}_{camada}"

    def tabela(self, camada: str, nome: str) -> str:
        return f"{self.catalog}.{self.schema(camada)}.{_validar_identificador(nome, 'tabela')}"

    @property
    def landing_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema('landing')}/arquivos"

    @property
    def relatorios_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema('landing')}/relatorios"


def criar_settings(ambiente: str, catalog: str, prefixo: str, run_id: str | None = None) -> Settings:
    """Cria o Settings; sem run_id (execucao local) gera um identificador unico."""
    rid = run_id if run_id and not run_id.startswith("{{") else f"local-{uuid.uuid4().hex[:12]}"
    return Settings(ambiente=ambiente, catalog=catalog, prefixo=prefixo, run_id=rid)
