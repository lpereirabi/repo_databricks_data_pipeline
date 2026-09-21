"""Ponto de entrada (`mkt-pipeline <comando>`). Cada comando e uma task do job Databricks."""

from __future__ import annotations

import argparse
import logging
import sys

from . import pipeline
from .config import criar_settings
from .control import carregar_parametros, etapa_ativa  # noqa: F401  (carregar_parametros: uso em debug)

COMANDOS = {
    "preparar": "preparar",
    "bronze": "bronze",
    "silver": "silver",
    "gold-dimensoes": "gold_dimensoes",
    "gold-fatos": "gold_fatos",
    "governanca": "governanca",
    "relatorio": "relatorio",
}


def _bool(valor: str) -> bool:
    return str(valor).strip().lower() in {"1", "true", "sim", "yes", "y"}


def criar_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="mkt-pipeline", description="Pipeline de Marketing (Medalhao)")
    sub = p.add_subparsers(dest="comando", required=True)
    for nome in COMANDOS:
        sp = sub.add_parser(nome)
        sp.add_argument("--ambiente", required=True, help="dev | stg | prod")
        sp.add_argument("--catalog", required=True, help="catalogo do Unity Catalog")
        sp.add_argument("--prefixo", required=True, help="prefixo dos schemas, ex.: mkt_stg")
        sp.add_argument("--run-id", default=None, help="id da execucao (no job: {{job.run_id}})")
        if nome == "preparar":
            sp.add_argument("--data-carga", default="auto", help="auto | AAAA-MM-DD")
            sp.add_argument("--reprocessar", default="false", help="true para refazer uma data ja carregada")
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout
    )
    args = criar_parser().parse_args(argv)
    cfg = criar_settings(args.ambiente, args.catalog, args.prefixo, args.run_id)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    if args.comando == "preparar":
        pipeline.preparar(spark, cfg, args.data_carga, _bool(args.reprocessar))
        return 0

    etapa = COMANDOS[args.comando]
    if not etapa_ativa(spark, cfg, etapa):
        logging.getLogger("mkt_pipeline").warning("Etapa '%s' desativada em ctl_etapa; nada a fazer", etapa)
        return 0
    getattr(pipeline, f"executar_{etapa}")(spark, cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
