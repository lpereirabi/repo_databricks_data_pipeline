import re
from decimal import Decimal

import pytest

from mkt_pipeline import ddl
from mkt_pipeline.cli import criar_parser
from mkt_pipeline.config import criar_settings
from mkt_pipeline.metadata import TABELAS
from mkt_pipeline.params import ETAPAS_PADRAO, PARAMETROS_PADRAO, Parametros
from mkt_pipeline.schema import COLUNAS_BRONZE, COLUNAS_ORIGEM


def cfg():
    return criar_settings("stg", "workspace", "mkt_stg", "123")


def test_settings_monta_nomes_por_ambiente():
    c = cfg()
    assert c.tabela("gold", "dim_cliente") == "workspace.mkt_stg_gold.dim_cliente"
    assert c.landing_path == "/Volumes/workspace/mkt_stg_landing/arquivos"
    assert c.relatorios_path == "/Volumes/workspace/mkt_stg_landing/relatorios"


@pytest.mark.parametrize("campo", ["catalog", "prefixo", "ambiente"])
def test_settings_rejeita_injecao_sql(campo):
    kw = {"ambiente": "stg", "catalog": "workspace", "prefixo": "mkt_stg", "run_id": "1"}
    kw[campo] = "x; DROP TABLE t"
    with pytest.raises(ValueError):
        criar_settings(**kw)


def test_run_id_de_template_nao_resolvido_vira_id_local():
    assert criar_settings("dev", "workspace", "mkt_dev", "{{job.run_id}}").run_id.startswith("local-")
    assert criar_settings("dev", "workspace", "mkt_dev", None).run_id.startswith("local-")


def test_parametros_padrao_tipados_e_com_fallback():
    p = Parametros.padrao()
    assert p.inteiro("max_tentativas") == 3
    assert p.decimal("limite_rejeicao_pct") == Decimal("10")
    assert p.texto("csv_separador") == ";"
    # parametro ausente na tabela cai no padrao; desconhecido e erro explicito
    assert Parametros({}).inteiro("max_tentativas") == 3
    with pytest.raises(KeyError):
        Parametros({}).texto("nao_existe")


def test_parametros_e_etapas_sem_duplicidade():
    nomes = [p[0] for p in PARAMETROS_PADRAO]
    assert len(nomes) == len(set(nomes))
    etapas = [e[0] for e in ETAPAS_PADRAO]
    assert len(etapas) == len(set(etapas))


def test_contrato_do_csv_tem_27_colunas_sem_duplicidade():
    assert len(COLUNAS_ORIGEM) == 27
    assert len(set(COLUNAS_BRONZE)) == 27


def test_ddl_cobre_todas_as_tabelas_e_respeita_o_ambiente():
    sql = "\n".join(ddl.todos_ddl(cfg()))
    for tab in TABELAS:
        assert f"workspace.mkt_stg_{tab.camada}.{tab.nome} " in sql, tab.nome
    assert "mkt_dev" not in sql and "mkt_prod" not in sql


def test_ddl_nao_usa_check_constraint_nem_particionamento():
    sql = "\n".join(ddl.todos_ddl(cfg())).upper()
    assert "CHECK (" not in sql  # UC do Free Edition so aceita PK/FK
    assert "PARTITIONED BY" not in sql  # liquid clustering no lugar


def test_fks_apontam_para_tabelas_e_colunas_do_ddl():
    sql_gold = "\n".join(ddl.ddl_gold(cfg()))
    fks = ddl.fks_gold(cfg())
    assert len(fks) == 15
    for nome, stmt in fks:
        assert nome.startswith("fk_")
        alvo = re.search(r"REFERENCES (\S+)\((\w+)\)", stmt)
        assert f"{alvo.group(1)} " in sql_gold  # tabela referenciada existe no DDL
        assert alvo.group(2) in sql_gold


def test_metadata_so_descreve_colunas_que_existem_no_ddl():
    sql = "\n".join(ddl.todos_ddl(cfg()))
    for tab in TABELAS:
        bloco = sql.split(f"{tab.camada}.{tab.nome} (", 1)
        assert len(bloco) == 2, tab.nome
        definicao = bloco[1].split("COMMENT", 1)[0]
        for coluna in tab.colunas:
            assert re.search(rf"\b{coluna}\b", definicao), f"{tab.nome}.{coluna} nao existe no DDL"


def test_parser_do_cli_aceita_os_comandos_do_job():
    parser = criar_parser()
    args = parser.parse_args(
        [
            "preparar",
            "--ambiente",
            "dev",
            "--catalog",
            "workspace",
            "--prefixo",
            "mkt_dev",
            "--data-carga",
            "2026-09-20",
        ]
    )
    assert args.comando == "preparar" and args.data_carga == "2026-09-20" and args.reprocessar == "false"
    for comando in ["bronze", "silver", "gold-dimensoes", "gold-fatos", "governanca", "relatorio"]:
        parser.parse_args([comando, "--ambiente", "dev", "--catalog", "c", "--prefixo", "p"])
