"""Parametrizacao: valores padrao + leitura da tabela ``ctl_parametro``.

Os padroes abaixo sao semeados na tabela (apenas o que ainda nao existe), entao qualquer ajuste feito
diretamente na tabela e preservado nas proximas execucoes. Nenhuma regra de negocio fica "hardcoded".
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

# (parametro, valor, tipo, descricao)
PARAMETROS_PADRAO: list[tuple[str, str, str, str]] = [
    ("arquivo_prefixo", "marketing", "texto", "Prefixo do arquivo de carga: <prefixo>_AAAAMMDD.<extensao>"),
    ("arquivo_extensao", "csv", "texto", "Extensao do arquivo de carga"),
    ("csv_separador", ";", "texto", "Separador de campos do CSV"),
    ("csv_encoding", "UTF-8", "texto", "Codificacao do CSV"),
    (
        "formato_data_origem",
        "d/M/yyyy",
        "texto",
        "Formato de Data Cadastro (aceita dia/mes com 1 ou 2 digitos)",
    ),
    (
        "ano_nascimento_min",
        "1900",
        "inteiro",
        "Ano de nascimento minimo valido; abaixo disso vira NULL + alerta",
    ),
    ("salario_max_outlier", "300000", "decimal", "Salario anual acima deste valor gera alerta de outlier"),
    ("limite_rejeicao_pct", "10", "decimal", "% maximo de linhas em quarentena; acima disso a carga falha"),
    (
        "max_tentativas",
        "3",
        "inteiro",
        "Tentativas automaticas por arquivo antes de exigir reprocesso manual",
    ),
    ("faixa_salarial_baixa_ate", "30000", "decimal", "Limite superior da faixa salarial Baixa"),
    ("faixa_salarial_media_ate", "60000", "decimal", "Limite superior da faixa salarial Media"),
    (
        "faixa_salarial_alta_ate",
        "100000",
        "decimal",
        "Limite superior da faixa salarial Alta (acima: Muito Alta)",
    ),
    ("dim_data_inicio", "2010-01-01", "texto", "Primeira data da dimensao de calendario"),
    ("dim_data_fim", "2035-12-31", "texto", "Ultima data da dimensao de calendario"),
]

ETAPAS_PADRAO: list[tuple[str, str, int, str]] = [
    ("preparar", "controle", 1, "Descobre arquivos na landing e registra as cargas pendentes"),
    ("bronze", "bronze", 2, "Ingere o CSV como esta (tudo string) + colunas de auditoria"),
    ("silver", "silver", 3, "Tipagem, limpeza, qualidade de dados e quarentena"),
    ("gold_dimensoes", "gold", 4, "Dimensoes do modelo estrela (SCD Tipo 2 no cliente)"),
    ("gold_fatos", "gold", 5, "Tabelas fato do modelo estrela"),
    ("governanca", "gold", 6, "Comentarios, tags, constraints e dicionario de dados"),
    ("relatorio", "gold", 7, "Gera a pagina HTML com relatorios e dashboards"),
]


@dataclass(frozen=True)
class Parametros:
    valores: dict[str, tuple[str, str]]  # nome -> (valor, tipo)

    @classmethod
    def padrao(cls) -> Parametros:
        return cls({nome: (valor, tipo) for nome, valor, tipo, _ in PARAMETROS_PADRAO})

    def texto(self, nome: str) -> str:
        return self._get(nome)[0]

    def inteiro(self, nome: str) -> int:
        return int(self._get(nome)[0])

    def decimal(self, nome: str) -> Decimal:
        return Decimal(self._get(nome)[0])

    def _get(self, nome: str) -> tuple[str, str]:
        if nome in self.valores:
            return self.valores[nome]
        padrao = Parametros.padrao().valores
        if nome in padrao:  # parametro inativo/removido da tabela: cai no padrao
            return padrao[nome]
        raise KeyError(f"parametro desconhecido: {nome}")
