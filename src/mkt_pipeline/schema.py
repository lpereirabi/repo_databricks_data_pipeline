"""Contrato de dados do arquivo de origem e dominios de referencia.

Fonte unica da verdade para: nomes originais das colunas do CSV, nomes padronizados (snake_case)
e listas de dominio usadas nas regras de qualidade e nas dimensoes.
"""

from __future__ import annotations

# Nome original no CSV -> nome padronizado. A ORDEM define a ordem das colunas na bronze.
COLUNAS_ORIGEM: dict[str, str] = {
    "ID": "id_cliente",
    "Ano Nascimento": "ano_nascimento",
    "Escolaridade": "escolaridade",
    "Estado Civil": "estado_civil",
    "Salario Anual": "salario_anual",
    "Filhos em Casa": "qtd_filhos",
    "Adolescentes em Casa": "qtd_adolescentes",
    "Data Cadastro": "data_cadastro",
    "Dias Desde Ultima Compra": "dias_desde_ultima_compra",
    "Gasto com Eletronicos": "gasto_eletronicos",
    "Gasto com Brinquedos": "gasto_brinquedos",
    "Gasto com Moveis": "gasto_moveis",
    "Gasto com Utilidades": "gasto_utilidades",
    "Gasto com Alimentos": "gasto_alimentos",
    "Gasto com Vestuario": "gasto_vestuario",
    "Numero de Compras com Desconto": "qtd_compras_desconto",
    "Numero de Compras na Web": "qtd_compras_web",
    "Numero de Compras via Catalogo": "qtd_compras_catalogo",
    "Numero de Compras na Loja": "qtd_compras_loja",
    "Numero Visitas WebSite Mes": "visitas_website_mes",
    "Compra na Campanha 1": "campanha_1_aceita",
    "Compra na Campanha 2": "campanha_2_aceita",
    "Compra na Campanha 3": "campanha_3_aceita",
    "Compra na Campanha 4": "campanha_4_aceita",
    "Compra na Campanha 5": "campanha_5_aceita",
    "Comprou": "comprou",
    "Pais": "pais",
}

COLUNAS_BRONZE: list[str] = list(COLUNAS_ORIGEM.values())

# Colunas de auditoria adicionadas na ingestao (prefixo "_" = metadado tecnico).
COLUNAS_META_BRONZE: list[str] = [
    "_data_carga",
    "_arquivo_origem",
    "_ingestao_ts",
    "_run_id",
    "_seq_linha",
    "_hash_linha",
]

# --- Grupos de colunas tipadas (silver) ---
COLUNAS_GASTO: list[str] = [
    "gasto_eletronicos",
    "gasto_brinquedos",
    "gasto_moveis",
    "gasto_utilidades",
    "gasto_alimentos",
    "gasto_vestuario",
]
COLUNAS_CANAL: list[str] = ["qtd_compras_web", "qtd_compras_catalogo", "qtd_compras_loja"]
COLUNAS_CAMPANHA: list[str] = [f"campanha_{n}_aceita" for n in range(1, 6)]
COLUNAS_CONTAGEM: list[str] = [
    "qtd_filhos",
    "qtd_adolescentes",
    "dias_desde_ultima_compra",
    "qtd_compras_desconto",
    "visitas_website_mes",
    *COLUNAS_CANAL,
]

# --- Dominios de referencia ---
# codigo -> nome de exibicao (o codigo alimenta a chave substituta da dimensao)
CATEGORIAS: dict[str, tuple[str, str]] = {
    "ELETRONICOS": ("Eletrônicos", "gasto_eletronicos"),
    "BRINQUEDOS": ("Brinquedos", "gasto_brinquedos"),
    "MOVEIS": ("Móveis", "gasto_moveis"),
    "UTILIDADES": ("Utilidades", "gasto_utilidades"),
    "ALIMENTOS": ("Alimentos", "gasto_alimentos"),
    "VESTUARIO": ("Vestuário", "gasto_vestuario"),
}
CANAIS: dict[str, tuple[str, str]] = {
    "WEB": ("Web", "qtd_compras_web"),
    "CATALOGO": ("Catálogo", "qtd_compras_catalogo"),
    "LOJA": ("Loja", "qtd_compras_loja"),
}
CAMPANHAS: dict[str, tuple[str, str]] = {
    f"CAMPANHA_{n}": (f"Campanha {n}", f"campanha_{n}_aceita") for n in range(1, 6)
}

# Escolaridade em ordem crescente (nivel_ordem na dimensao); fora da lista = alerta de dominio.
ESCOLARIDADES: dict[str, int] = {
    "Primeiro Grau": 1,
    "Segundo Grau": 2,
    "Curso Superior": 3,
    "Mestrado": 4,
    "Doutorado": 5,
}
ESTADOS_CIVIS: list[str] = ["Solteiro", "Casado", "Divorciado"]

NAO_INFORMADO = "Não Informado"

# Atributos que, ao mudarem, geram uma nova versao do cliente (SCD Tipo 2).
ATRIBUTOS_SCD2: list[str] = [
    "ano_nascimento",
    "escolaridade",
    "estado_civil",
    "pais",
    "salario_anual",
    "qtd_filhos",
    "qtd_adolescentes",
]

# --- Estados de uma carga (ctl_carga_arquivo.status) ---
PENDENTE = "PENDENTE"
BRONZE_OK = "BRONZE_OK"
SILVER_OK = "SILVER_OK"
DIMENSOES_OK = "DIMENSOES_OK"
SUCESSO = "SUCESSO"
ERRO_BRONZE = "ERRO_BRONZE"
ERRO_SILVER = "ERRO_SILVER"
ERRO_DIMENSOES = "ERRO_DIMENSOES"
ERRO_FATOS = "ERRO_FATOS"
ERRO_FORA_DE_ORDEM = "ERRO_FORA_DE_ORDEM"
