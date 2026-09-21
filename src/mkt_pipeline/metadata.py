"""Metadados de governanca: descricao de tabelas/colunas e classificacao de sensibilidade.

E a fonte do dicionario de dados (`ctl.dicionario_dados`), dos comentarios no Unity Catalog e das tags.
Classificacao: publico | interno | confidencial (dado pessoal/financeiro; restringir acesso e mascarar em BI aberto).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .schema import CAMPANHAS, CANAIS, CATEGORIAS, COLUNAS_ORIGEM

CONFIDENCIAL = {"id_cliente", "ano_nascimento", "salario_anual", "idade"}


def classificar(coluna: str) -> str:
    if coluna in CONFIDENCIAL:
        return "confidencial"
    if coluna.startswith("_"):
        return "interno"
    return "interno"


@dataclass(frozen=True)
class TabelaMeta:
    camada: str
    nome: str
    descricao: str
    colunas: dict[str, str] = field(default_factory=dict)  # coluna -> descricao


_BRONZE = {
    novo: f"Coluna '{orig}' do CSV, sem nenhum tratamento (STRING)" for orig, novo in COLUNAS_ORIGEM.items()
}
_BRONZE.update(
    {
        "_data_carga": "Data da carga (extraida do nome do arquivo marketing_AAAAMMDD.csv)",
        "_arquivo_origem": "Caminho completo do arquivo de origem no volume",
        "_ingestao_ts": "Momento da ingestao",
        "_run_id": "Identificador da execucao do job que gravou a linha",
        "_seq_linha": "Sequencial tecnico da linha dentro da carga",
        "_hash_linha": "SHA-256 do conteudo original da linha (deteccao de mudanca)",
    }
)

_SILVER = {
    "id_cliente": "Identificador do cliente na origem (chave natural)",
    "ano_nascimento": "Ano de nascimento; NULL quando invalido (ver dq_alertas)",
    "escolaridade": "Nivel de escolaridade padronizado",
    "estado_civil": "Estado civil padronizado",
    "pais": "Pais de residencia",
    "salario_anual": "Salario anual em moeda local",
    "qtd_filhos": "Filhos em casa",
    "qtd_adolescentes": "Adolescentes em casa",
    "data_cadastro": "Data de cadastro do cliente (convertida de dd/MM/yyyy)",
    "dias_desde_ultima_compra": "Recencia: dias desde a ultima compra",
    "total_gasto": "Soma dos gastos nas 6 categorias",
    "qtd_compras_desconto": "Compras realizadas com desconto",
    "total_compras": "Compras em todos os canais (web + catalogo + loja)",
    "visitas_website_mes": "Visitas ao website no mes",
    "qtd_campanhas_aceitas": "Quantidade de campanhas (1 a 5) em que o cliente comprou",
    "comprou": "Indicador de conversao (TRUE = comprou)",
    "hash_conteudo": "SHA-256 dos atributos exceto o ID (deteccao de clientes clonados)",
    "dq_alertas": "Codigos das regras de qualidade de alerta violadas pela linha",
    "_data_carga": "Data da carga de origem",
    "_run_id": "Execucao que gravou a linha",
    "_processado_ts": "Momento do processamento",
}
for _c in ("eletronicos", "brinquedos", "moveis", "utilidades", "alimentos", "vestuario"):
    _SILVER[f"gasto_{_c}"] = f"Gasto do cliente na categoria {_c}"
for _c in ("web", "catalogo", "loja"):
    _SILVER[f"qtd_compras_{_c}"] = f"Compras no canal {_c}"
for _n in range(1, 6):
    _SILVER[f"campanha_{_n}_aceita"] = f"TRUE se comprou na campanha {_n}"

_SK = "Chave substituta (hash deterministico xxhash64 do valor de negocio)"

TABELAS: list[TabelaMeta] = [
    TabelaMeta("bronze", "marketing_raw", "Copia fiel do CSV de marketing + auditoria (linhagem)", _BRONZE),
    TabelaMeta(
        "silver",
        "marketing_cliente",
        "Clientes tipados, limpos e enriquecidos; 1 linha por cliente por carga",
        _SILVER,
    ),
    TabelaMeta(
        "silver",
        "marketing_quarentena",
        "Linhas rejeitadas por regra bloqueante de qualidade, com motivo e linha original (JSON)",
        {
            "id_cliente_raw": "ID como veio no arquivo",
            "motivos": "Codigos das regras violadas",
            "linha_original": "Linha original em JSON, para analise/reprocesso",
        },
    ),
    TabelaMeta(
        "gold",
        "dim_cliente",
        "Dimensao cliente SCD Tipo 2 (atributos rastreados: nascimento, escolaridade, estado civil, pais, salario, filhos)",
        {
            "sk_cliente": "Chave substituta da VERSAO do cliente (hash de id_cliente + inicio_vigencia)",
            "id_cliente": "Chave natural (origem)",
            "faixa_salarial": "Baixa/Media/Alta/Muito Alta conforme parametros",
            "hash_atributos": "SHA-256 dos atributos rastreados; mudou = nova versao",
            "inicio_vigencia": "Primeira data de carga em que esta versao valeu",
            "fim_vigencia": "Ultima data de vigencia (9999-12-31 se atual)",
            "flag_atual": "TRUE na versao vigente",
        },
    ),
    TabelaMeta(
        "gold", "dim_data", "Dimensao calendario (sk_data = AAAAMMDD)", {"sk_data": "AAAAMMDD como inteiro"}
    ),
    TabelaMeta("gold", "dim_pais", "Dimensao de pais", {"sk_pais": _SK}),
    TabelaMeta(
        "gold",
        "dim_escolaridade",
        "Dimensao de escolaridade",
        {"sk_escolaridade": _SK, "nivel_ordem": "1 (menor) a 5 (maior grau); 99 = desconhecido"},
    ),
    TabelaMeta("gold", "dim_estado_civil", "Dimensao de estado civil", {"sk_estado_civil": _SK}),
    TabelaMeta(
        "gold",
        "dim_categoria_produto",
        "Dimensao de categoria de produto",
        {"sk_categoria": _SK, "cod_categoria": "Codigo: " + ", ".join(CATEGORIAS)},
    ),
    TabelaMeta(
        "gold",
        "dim_canal",
        "Dimensao de canal de compra",
        {"sk_canal": _SK, "cod_canal": "Codigo: " + ", ".join(CANAIS)},
    ),
    TabelaMeta(
        "gold",
        "dim_campanha",
        "Dimensao de campanha de marketing",
        {"sk_campanha": _SK, "cod_campanha": "Codigo: " + ", ".join(CAMPANHAS)},
    ),
    TabelaMeta(
        "gold",
        "fato_cliente_snapshot",
        "Fato snapshot: 1 linha por cliente por carga. Medidas: gasto, compras, recencia, visitas, campanhas",
        {
            "sk_data_carga": "FK dim_data: data da carga",
            "idade": "Idade na data da carga",
            "total_gasto": "Soma dos gastos (medida aditiva)",
            "comprou": "1 = converteu, 0 = nao",
        },
    ),
    TabelaMeta(
        "gold",
        "fato_gasto_categoria",
        "Fato: gasto por cliente x categoria x carga",
        {"valor_gasto": "Valor gasto (medida aditiva)"},
    ),
    TabelaMeta(
        "gold",
        "fato_compras_canal",
        "Fato: compras por cliente x canal x carga",
        {"qtd_compras": "Quantidade de compras (medida aditiva)"},
    ),
    TabelaMeta(
        "gold",
        "fato_resposta_campanha",
        "Fato: aceite de campanha por cliente x campanha x carga",
        {"aceitou": "1 = comprou na campanha, 0 = nao"},
    ),
    TabelaMeta("ctl", "ctl_parametro", "Parametros do pipeline (editaveis sem deploy)"),
    TabelaMeta("ctl", "ctl_etapa", "Etapas do pipeline; ativo=false desliga a etapa"),
    TabelaMeta("ctl", "ctl_carga_arquivo", "Controle de cada arquivo/data de carga e seu estado"),
    TabelaMeta("ctl", "log_execucao", "Log de integracao (uma linha por etapa/carga)"),
    TabelaMeta("ctl", "log_erro", "Log de erros e avisos com stacktrace"),
    TabelaMeta("ctl", "dq_resultado", "Resultado das regras de qualidade por carga"),
    TabelaMeta("ctl", "dicionario_dados", "Dicionario de dados (gerado)"),
]
