"""Etapas do pipeline (uma funcao por task do job)."""

from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import SparkSession

from . import governance
from .bronze import carregar_bronze
from .config import Settings
from .control import (
    Execucao,
    carregar_parametros,
    estado_cargas,
    registrar_cargas,
    registrar_erro,
    setup,
)
from .files import listar_arquivos, sha256_arquivo
from .gold import processar_dimensoes, processar_fatos, semear_dimensoes_estaticas
from .runner import processar_cargas
from .schema import (
    BRONZE_OK,
    DIMENSOES_OK,
    ERRO_BRONZE,
    ERRO_DIMENSOES,
    ERRO_FATOS,
    ERRO_FORA_DE_ORDEM,
    ERRO_SILVER,
    PENDENTE,
    SILVER_OK,
    SUCESSO,
)
from .silver import processar_silver

log = logging.getLogger("mkt_pipeline")


def preparar(spark: SparkSession, cfg: Settings, data_carga: str = "auto", reprocessar: bool = False) -> None:
    """Descobre os arquivos da landing e registra em ctl_carga_arquivo o que sera processado nesta execucao.

    data_carga = "auto"       -> todo arquivo ainda nao carregado (ou com erro e tentativas restantes)
    data_carga = "AAAA-MM-DD" -> apenas aquela data; com reprocessar=true refaz mesmo se ja tiver sucesso
    """
    setup(spark, cfg)
    params = carregar_parametros(spark, cfg)
    with Execucao(spark, cfg, "preparar", "controle", cfg.tabela("ctl", "ctl_carga_arquivo")) as ex:
        prefixo, ext = params.texto("arquivo_prefixo"), params.texto("arquivo_extensao")
        max_tentativas = params.inteiro("max_tentativas")
        validos, invalidos = listar_arquivos(cfg.landing_path, prefixo, ext)
        for nome in invalidos:
            registrar_erro(
                spark,
                cfg,
                "preparar",
                "ARQUIVO_NOME_INVALIDO",
                f"'{nome}' ignorado: o nome deve ser {prefixo}_AAAAMMDD.{ext}",
                severidade="AVISO",
            )

        estado = estado_cargas(spark, cfg)
        ultima_sucesso = max((d for d, c in estado.items() if c.status == SUCESSO), default=None)
        explicito = data_carga.strip().lower() != "auto"

        if explicito:
            alvo = date.fromisoformat(data_carga.strip())
            candidatos = [a for a in validos if a.data_carga == alvo]
            if not candidatos:
                registrar_erro(
                    spark,
                    cfg,
                    "preparar",
                    "ARQUIVO_NAO_ENCONTRADO",
                    f"Nao existe {prefixo}_{alvo:%Y%m%d}.{ext} em {cfg.landing_path}",
                    alvo,
                )
                raise FileNotFoundError(f"Arquivo da carga {alvo} nao encontrado em {cfg.landing_path}")
            if reprocessar and ultima_sucesso and alvo < ultima_sucesso:
                raise ValueError(
                    f"REPROCESSO_FORA_DE_ORDEM: {alvo} e anterior a ultima carga com sucesso ({ultima_sucesso}); "
                    "o SCD Tipo 2 exige ordem cronologica"
                )
        else:
            candidatos = validos

        hashes = {
            r["data_carga"]: r["hash_sha256"]
            for r in spark.table(cfg.tabela("ctl", "ctl_carga_arquivo")).collect()
        }
        plano: list[dict] = []
        for arq in candidatos:
            atual = estado.get(arq.data_carga)
            if atual and atual.status == SUCESSO and not (explicito and reprocessar):
                continue
            if not explicito and atual and atual.status == ERRO_FORA_DE_ORDEM:
                continue
            if (
                not explicito
                and atual
                and atual.status.startswith("ERRO")
                and atual.tentativas >= max_tentativas
            ):
                registrar_erro(
                    spark,
                    cfg,
                    "preparar",
                    "MAX_TENTATIVAS",
                    f"{arq.nome}: {atual.tentativas} tentativas; use data_carga={arq.data_carga} e reprocessar=true",
                    arq.data_carga,
                    severidade="AVISO",
                )
                continue

            item = {
                "data_carga": arq.data_carga,
                "arquivo": arq.nome,
                "caminho": arq.caminho,
                "tamanho_bytes": arq.tamanho_bytes,
                "hash_sha256": sha256_arquivo(arq.caminho),
                "tentativas": 1 if (explicito and reprocessar) else (atual.tentativas if atual else 0) + 1,
                "status": PENDENTE,
                "mensagem": None,
            }
            fora_de_ordem = ultima_sucesso is not None and arq.data_carga < ultima_sucesso and atual is None
            if fora_de_ordem:
                item.update(
                    status=ERRO_FORA_DE_ORDEM,
                    mensagem=f"Anterior a ultima carga com sucesso ({ultima_sucesso})",
                )
                registrar_erro(
                    spark,
                    cfg,
                    "preparar",
                    "ARQUIVO_FORA_DE_ORDEM",
                    item["mensagem"],
                    arq.data_carga,
                    severidade="AVISO",
                )
            repetido = [d for d, h in hashes.items() if h == item["hash_sha256"] and d != arq.data_carga]
            if repetido and not fora_de_ordem:
                registrar_erro(
                    spark,
                    cfg,
                    "preparar",
                    "ARQUIVO_CONTEUDO_DUPLICADO",
                    f"{arq.nome} tem o mesmo conteudo da(s) carga(s) {sorted(repetido)}",
                    arq.data_carga,
                    severidade="AVISO",
                )
            plano.append(item)

        registrar_cargas(spark, cfg, plano)
        pendentes = sum(1 for p in plano if p["status"] == PENDENTE)
        ex.registrar(
            lidas=len(validos),
            gravadas=pendentes,
            rejeitadas=len(plano) - pendentes,
            mensagem=f"{pendentes} carga(s) para processar",
        )


def executar_bronze(spark: SparkSession, cfg: Settings) -> None:
    params = carregar_parametros(spark, cfg)
    processar_cargas(
        spark,
        cfg,
        etapa="bronze",
        camada="bronze",
        tabela=cfg.tabela("bronze", "marketing_raw"),
        status_entrada=PENDENTE,
        status_sucesso=BRONZE_OK,
        status_erro=ERRO_BRONZE,
        func=lambda c: carregar_bronze(spark, cfg, params, c),
    )


def executar_silver(spark: SparkSession, cfg: Settings) -> None:
    params = carregar_parametros(spark, cfg)
    processar_cargas(
        spark,
        cfg,
        etapa="silver",
        camada="silver",
        tabela=cfg.tabela("silver", "marketing_cliente"),
        status_entrada=BRONZE_OK,
        status_sucesso=SILVER_OK,
        status_erro=ERRO_SILVER,
        func=lambda c: processar_silver(spark, cfg, params, c),
    )


def executar_gold_dimensoes(spark: SparkSession, cfg: Settings) -> None:
    params = carregar_parametros(spark, cfg)
    with Execucao(spark, cfg, "gold_dimensoes", "gold") as ex:
        semear_dimensoes_estaticas(spark, cfg, params)
        ex.registrar(mensagem="dimensoes estaticas e calendario garantidos")
    processar_cargas(
        spark,
        cfg,
        etapa="gold_dimensoes",
        camada="gold",
        tabela=cfg.tabela("gold", "dim_cliente"),
        status_entrada=SILVER_OK,
        status_sucesso=DIMENSOES_OK,
        status_erro=ERRO_DIMENSOES,
        func=lambda c: processar_dimensoes(spark, cfg, params, c),
    )


def executar_gold_fatos(spark: SparkSession, cfg: Settings) -> None:
    processar_cargas(
        spark,
        cfg,
        etapa="gold_fatos",
        camada="gold",
        tabela=cfg.tabela("gold", "fato_cliente_snapshot"),
        status_entrada=DIMENSOES_OK,
        status_sucesso=SUCESSO,
        status_erro=ERRO_FATOS,
        func=lambda c: processar_fatos(spark, cfg, c),
    )


def executar_governanca(spark: SparkSession, cfg: Settings) -> None:
    with Execucao(spark, cfg, "governanca", "gold") as ex:
        n_ok, n_falha = governance.aplicar_comentarios_e_tags(spark, cfg)
        fk_ok, fk_falha = governance.aplicar_constraints(spark, cfg)
        n_dic = governance.gerar_dicionario(spark, cfg)
        ex.registrar(
            gravadas=n_dic,
            rejeitadas=n_falha + fk_falha,
            mensagem=f"{n_ok} comentarios/tags e {fk_ok} FKs aplicados; {n_falha + fk_falha} avisos; {n_dic} colunas no dicionario",
        )


def executar_relatorio(spark: SparkSession, cfg: Settings) -> None:
    from .report import gerar_relatorio  # import tardio: o modulo so e necessario nesta etapa

    with Execucao(spark, cfg, "relatorio", "gold") as ex:
        caminho = gerar_relatorio(spark, cfg)
        ex.registrar(mensagem=f"relatorio gravado em {caminho}")
