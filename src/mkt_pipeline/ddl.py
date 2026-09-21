"""DDL de todas as tabelas Delta (bronze, silver, gold e controle). Idempotente: CREATE TABLE IF NOT EXISTS.

Boas praticas aplicadas: Unity Catalog (nome de 3 partes), liquid clustering (CLUSTER BY) no lugar de
particionamento, Change Data Feed nas camadas consumidas e chaves primarias/estrangeiras informativas.
Obs.: o Unity Catalog do Free Edition so aceita constraints PRIMARY KEY/FOREIGN KEY (sem CHECK); as regras de
dominio ficam em dq.py (quarentena), nao na tabela.
"""

from __future__ import annotations

from .config import Settings
from .schema import COLUNAS_BRONZE

_PROPS_CDF = "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
_PROPS_PADRAO = "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')"


def ddl_controle(cfg: Settings) -> list[str]:
    t = cfg.tabela
    return [
        f"""CREATE TABLE IF NOT EXISTS {t("ctl", "ctl_parametro")} (
  parametro STRING NOT NULL,
  valor STRING,
  tipo STRING,
  descricao STRING,
  ativo BOOLEAN,
  atualizado_em TIMESTAMP,
  atualizado_por STRING
) COMMENT 'Parametrizacao do pipeline (editavel sem deploy)' {_PROPS_PADRAO}""",
        f"""CREATE TABLE IF NOT EXISTS {t("ctl", "ctl_etapa")} (
  etapa STRING NOT NULL,
  camada STRING,
  ordem INT,
  ativo BOOLEAN,
  descricao STRING
) COMMENT 'Etapas do pipeline; ativo=false desliga a etapa (kill switch)' {_PROPS_PADRAO}""",
        f"""CREATE TABLE IF NOT EXISTS {t("ctl", "ctl_carga_arquivo")} (
  data_carga DATE NOT NULL,
  arquivo STRING,
  caminho STRING,
  tamanho_bytes BIGINT,
  hash_sha256 STRING,
  status STRING,
  tentativas INT,
  linhas_bronze BIGINT,
  linhas_silver BIGINT,
  linhas_quarentena BIGINT,
  run_id_ultimo STRING,
  mensagem STRING,
  criado_em TIMESTAMP,
  atualizado_em TIMESTAMP
) COMMENT 'Registro de cada arquivo/data de carga e seu estado na maquina de estados do pipeline'
{_PROPS_PADRAO}""",
        f"""CREATE TABLE IF NOT EXISTS {t("ctl", "log_execucao")} (
  run_id STRING,
  ambiente STRING,
  etapa STRING,
  camada STRING,
  tabela_destino STRING,
  data_carga DATE,
  status STRING,
  inicio_ts TIMESTAMP,
  fim_ts TIMESTAMP,
  duracao_seg DOUBLE,
  linhas_lidas BIGINT,
  linhas_gravadas BIGINT,
  linhas_rejeitadas BIGINT,
  mensagem STRING,
  usuario STRING,
  versao_pacote STRING
) COMMENT 'Log de integracao: uma linha por etapa/carga executada' {_PROPS_PADRAO}""",
        f"""CREATE TABLE IF NOT EXISTS {t("ctl", "log_erro")} (
  erro_id STRING,
  run_id STRING,
  ambiente STRING,
  etapa STRING,
  data_carga DATE,
  severidade STRING,
  tipo_erro STRING,
  mensagem STRING,
  stacktrace STRING,
  erro_ts TIMESTAMP
) COMMENT 'Log de erros e avisos com stacktrace' {_PROPS_PADRAO}""",
        f"""CREATE TABLE IF NOT EXISTS {t("ctl", "dq_resultado")} (
  run_id STRING,
  data_carga DATE,
  tabela STRING,
  regra STRING,
  descricao STRING,
  severidade STRING,
  acao STRING,
  total BIGINT,
  falhas BIGINT,
  pct_falha DOUBLE,
  avaliado_ts TIMESTAMP
) COMMENT 'Resultado das regras de qualidade de dados por carga' {_PROPS_PADRAO}""",
        f"""CREATE TABLE IF NOT EXISTS {t("ctl", "dicionario_dados")} (
  tabela STRING,
  coluna STRING,
  tipo STRING,
  descricao STRING,
  camada STRING,
  classificacao STRING,
  atualizado_em TIMESTAMP
) COMMENT 'Dicionario de dados gerado a partir de metadata.py' {_PROPS_PADRAO}""",
    ]


def ddl_bronze(cfg: Settings) -> list[str]:
    colunas = ",\n  ".join(f"{c} STRING" for c in COLUNAS_BRONZE)
    return [
        f"""CREATE TABLE IF NOT EXISTS {cfg.tabela("bronze", "marketing_raw")} (
  {colunas},
  _data_carga DATE NOT NULL,
  _arquivo_origem STRING,
  _ingestao_ts TIMESTAMP,
  _run_id STRING,
  _seq_linha BIGINT,
  _hash_linha STRING
) CLUSTER BY (_data_carga)
COMMENT 'Bronze: copia fiel do CSV (tudo STRING) + auditoria. Nunca corrigir dados aqui.' {_PROPS_PADRAO}"""
    ]


def ddl_silver(cfg: Settings) -> list[str]:
    return [
        f"""CREATE TABLE IF NOT EXISTS {cfg.tabela("silver", "marketing_cliente")} (
  id_cliente BIGINT NOT NULL,
  ano_nascimento INT,
  escolaridade STRING,
  estado_civil STRING,
  pais STRING,
  salario_anual DECIMAL(12,2),
  qtd_filhos INT,
  qtd_adolescentes INT,
  data_cadastro DATE,
  dias_desde_ultima_compra INT,
  gasto_eletronicos DECIMAL(12,2),
  gasto_brinquedos DECIMAL(12,2),
  gasto_moveis DECIMAL(12,2),
  gasto_utilidades DECIMAL(12,2),
  gasto_alimentos DECIMAL(12,2),
  gasto_vestuario DECIMAL(12,2),
  total_gasto DECIMAL(14,2),
  qtd_compras_desconto INT,
  qtd_compras_web INT,
  qtd_compras_catalogo INT,
  qtd_compras_loja INT,
  total_compras INT,
  visitas_website_mes INT,
  campanha_1_aceita BOOLEAN,
  campanha_2_aceita BOOLEAN,
  campanha_3_aceita BOOLEAN,
  campanha_4_aceita BOOLEAN,
  campanha_5_aceita BOOLEAN,
  qtd_campanhas_aceitas INT,
  comprou BOOLEAN,
  hash_conteudo STRING,
  dq_alertas ARRAY<STRING>,
  _data_carga DATE NOT NULL,
  _run_id STRING,
  _processado_ts TIMESTAMP
) CLUSTER BY (_data_carga)
COMMENT 'Silver: clientes tipados, limpos e enriquecidos; 1 linha por cliente por carga'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.dataSkippingNumIndexedCols' = '-1')""",
        f"""CREATE TABLE IF NOT EXISTS {cfg.tabela("silver", "marketing_quarentena")} (
  id_cliente_raw STRING,
  motivos ARRAY<STRING>,
  linha_original STRING,
  _data_carga DATE NOT NULL,
  _run_id STRING,
  _processado_ts TIMESTAMP
) CLUSTER BY (_data_carga)
COMMENT 'Linhas rejeitadas por regras bloqueantes de qualidade, com o motivo' {_PROPS_PADRAO}""",
    ]


def ddl_gold(cfg: Settings) -> list[str]:
    t = lambda n: cfg.tabela("gold", n)  # noqa: E731
    dim_simples = [
        (
            "dim_pais",
            "sk_pais BIGINT NOT NULL, pais STRING NOT NULL, CONSTRAINT pk_dim_pais PRIMARY KEY (sk_pais)",
            "Dimensao de pais",
        ),
        (
            "dim_escolaridade",
            "sk_escolaridade BIGINT NOT NULL, escolaridade STRING NOT NULL, nivel_ordem INT, "
            "CONSTRAINT pk_dim_escolaridade PRIMARY KEY (sk_escolaridade)",
            "Dimensao de escolaridade (nivel_ordem permite ordenar do menor ao maior grau)",
        ),
        (
            "dim_estado_civil",
            "sk_estado_civil BIGINT NOT NULL, estado_civil STRING NOT NULL, "
            "CONSTRAINT pk_dim_estado_civil PRIMARY KEY (sk_estado_civil)",
            "Dimensao de estado civil",
        ),
        (
            "dim_categoria_produto",
            "sk_categoria BIGINT NOT NULL, cod_categoria STRING NOT NULL, categoria STRING, "
            "CONSTRAINT pk_dim_categoria PRIMARY KEY (sk_categoria)",
            "Dimensao de categoria de produto (origem das colunas Gasto com ...)",
        ),
        (
            "dim_canal",
            "sk_canal BIGINT NOT NULL, cod_canal STRING NOT NULL, canal STRING, "
            "CONSTRAINT pk_dim_canal PRIMARY KEY (sk_canal)",
            "Dimensao de canal de compra (Web, Catalogo, Loja)",
        ),
        (
            "dim_campanha",
            "sk_campanha BIGINT NOT NULL, cod_campanha STRING NOT NULL, campanha STRING, "
            "CONSTRAINT pk_dim_campanha PRIMARY KEY (sk_campanha)",
            "Dimensao de campanha de marketing (1 a 5)",
        ),
    ]
    stmts = [
        f"CREATE TABLE IF NOT EXISTS {t(nome)} ({cols}) COMMENT '{com}' {_PROPS_CDF}"
        for nome, cols, com in dim_simples
    ]
    stmts.append(
        f"""CREATE TABLE IF NOT EXISTS {t("dim_data")} (
  sk_data INT NOT NULL,
  data DATE NOT NULL,
  ano INT,
  semestre INT,
  trimestre INT,
  mes INT,
  nome_mes STRING,
  ano_mes STRING,
  dia INT,
  dia_semana INT,
  nome_dia_semana STRING,
  semana_ano INT,
  fim_de_semana BOOLEAN,
  CONSTRAINT pk_dim_data PRIMARY KEY (sk_data)
) COMMENT 'Dimensao calendario (sk_data = AAAAMMDD)' {_PROPS_CDF}"""
    )
    stmts.append(
        f"""CREATE TABLE IF NOT EXISTS {t("dim_cliente")} (
  sk_cliente BIGINT NOT NULL,
  id_cliente BIGINT NOT NULL,
  ano_nascimento INT,
  escolaridade STRING,
  estado_civil STRING,
  pais STRING,
  salario_anual DECIMAL(12,2),
  faixa_salarial STRING,
  qtd_filhos INT,
  qtd_adolescentes INT,
  data_cadastro DATE,
  hash_atributos STRING,
  inicio_vigencia DATE NOT NULL,
  fim_vigencia DATE NOT NULL,
  flag_atual BOOLEAN NOT NULL,
  _run_id STRING,
  _atualizado_ts TIMESTAMP,
  CONSTRAINT pk_dim_cliente PRIMARY KEY (sk_cliente)
) CLUSTER BY (id_cliente)
COMMENT 'Dimensao cliente SCD Tipo 2: uma linha por versao dos atributos rastreados' {_PROPS_CDF}"""
    )

    fatos = {
        "fato_cliente_snapshot": (
            """sk_cliente BIGINT NOT NULL, id_cliente BIGINT NOT NULL, sk_data_carga INT NOT NULL,
  sk_data_cadastro INT, sk_pais BIGINT, sk_escolaridade BIGINT, sk_estado_civil BIGINT,
  idade INT, faixa_etaria STRING, salario_anual DECIMAL(12,2),
  dias_desde_ultima_compra INT, visitas_website_mes INT, qtd_compras_desconto INT,
  qtd_compras_web INT, qtd_compras_catalogo INT, qtd_compras_loja INT, total_compras INT,
  total_gasto DECIMAL(14,2), qtd_campanhas_aceitas INT, comprou INT,
  _run_id STRING, _carga_ts TIMESTAMP""",
            "Fato snapshot: 1 linha por cliente por carga com as medidas consolidadas",
        ),
        "fato_gasto_categoria": (
            """sk_cliente BIGINT NOT NULL, sk_data_carga INT NOT NULL, sk_categoria BIGINT NOT NULL,
  valor_gasto DECIMAL(12,2), _run_id STRING, _carga_ts TIMESTAMP""",
            "Fato: gasto por cliente x categoria de produto x carga",
        ),
        "fato_compras_canal": (
            """sk_cliente BIGINT NOT NULL, sk_data_carga INT NOT NULL, sk_canal BIGINT NOT NULL,
  qtd_compras INT, _run_id STRING, _carga_ts TIMESTAMP""",
            "Fato: quantidade de compras por cliente x canal x carga",
        ),
        "fato_resposta_campanha": (
            """sk_cliente BIGINT NOT NULL, sk_data_carga INT NOT NULL, sk_campanha BIGINT NOT NULL,
  aceitou INT, _run_id STRING, _carga_ts TIMESTAMP""",
            "Fato: aceite (1/0) de cada campanha por cliente x carga",
        ),
    }
    for nome, (cols, com) in fatos.items():
        stmts.append(
            f"CREATE TABLE IF NOT EXISTS {t(nome)} ({cols}) CLUSTER BY (sk_data_carga) "
            f"COMMENT '{com}' {_PROPS_CDF}"
        )
    return stmts


def todos_ddl(cfg: Settings) -> list[str]:
    return [*ddl_controle(cfg), *ddl_bronze(cfg), *ddl_silver(cfg), *ddl_gold(cfg)]


# Relacionamentos (chaves estrangeiras informativas: ajudam BI e documentacao; nao sao validadas).
def fks_gold(cfg: Settings) -> list[tuple[str, str]]:
    """(nome_da_constraint, ddl do ALTER TABLE ... ADD CONSTRAINT)"""
    g = lambda n: cfg.tabela("gold", n)  # noqa: E731
    rels = [
        ("fato_cliente_snapshot", "sk_cliente", "dim_cliente", "sk_cliente"),
        ("fato_cliente_snapshot", "sk_data_carga", "dim_data", "sk_data"),
        ("fato_cliente_snapshot", "sk_data_cadastro", "dim_data", "sk_data"),
        ("fato_cliente_snapshot", "sk_pais", "dim_pais", "sk_pais"),
        ("fato_cliente_snapshot", "sk_escolaridade", "dim_escolaridade", "sk_escolaridade"),
        ("fato_cliente_snapshot", "sk_estado_civil", "dim_estado_civil", "sk_estado_civil"),
        ("fato_gasto_categoria", "sk_cliente", "dim_cliente", "sk_cliente"),
        ("fato_gasto_categoria", "sk_data_carga", "dim_data", "sk_data"),
        ("fato_gasto_categoria", "sk_categoria", "dim_categoria_produto", "sk_categoria"),
        ("fato_compras_canal", "sk_cliente", "dim_cliente", "sk_cliente"),
        ("fato_compras_canal", "sk_data_carga", "dim_data", "sk_data"),
        ("fato_compras_canal", "sk_canal", "dim_canal", "sk_canal"),
        ("fato_resposta_campanha", "sk_cliente", "dim_cliente", "sk_cliente"),
        ("fato_resposta_campanha", "sk_data_carga", "dim_data", "sk_data"),
        ("fato_resposta_campanha", "sk_campanha", "dim_campanha", "sk_campanha"),
    ]
    return [
        (
            f"fk_{fato}_{col}",
            f"ALTER TABLE {g(fato)} ADD CONSTRAINT fk_{fato}_{col} FOREIGN KEY ({col}) "
            f"REFERENCES {g(dim)}({dcol})",
        )
        for fato, col, dim, dcol in rels
    ]
