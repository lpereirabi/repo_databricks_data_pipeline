"""Tabelas de controle: setup, parametros, maquina de estados das cargas, logs de execucao e de erro."""

from __future__ import annotations

import logging
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from . import ddl
from .config import Settings
from .params import ETAPAS_PADRAO, PARAMETROS_PADRAO, Parametros

log = logging.getLogger("mkt_pipeline")


def agora() -> datetime:
    return datetime.now(timezone.utc)


def _versao_pacote() -> str:
    try:
        return version("mkt-pipeline")
    except PackageNotFoundError:
        return "dev"


def _usuario(spark: SparkSession) -> str:
    try:
        return spark.sql("SELECT current_user()").first()[0]
    except Exception:  # noqa: BLE001 - log nunca pode derrubar o pipeline
        return "desconhecido"


# --------------------------------------------------------------------------------------------------
# Setup (idempotente)
# --------------------------------------------------------------------------------------------------
def criar_objetos(spark: SparkSession, cfg: Settings) -> None:
    for stmt in ddl.todos_ddl(cfg):
        spark.sql(stmt)


def semear_parametros(spark: SparkSession, cfg: Settings) -> None:
    """Insere so o que falta (WHEN NOT MATCHED): ajustes feitos na tabela nunca sao sobrescritos."""
    tabela = cfg.tabela("ctl", "ctl_parametro")
    df = spark.createDataFrame(
        [(p, v, t, d) for p, v, t, d in PARAMETROS_PADRAO],
        "parametro STRING, valor STRING, tipo STRING, descricao STRING",
    )
    df.createOrReplaceTempView("_seed_parametro")
    spark.sql(
        f"""MERGE INTO {tabela} AS t USING _seed_parametro AS s ON t.parametro = s.parametro
WHEN NOT MATCHED THEN INSERT (parametro, valor, tipo, descricao, ativo, atualizado_em, atualizado_por)
VALUES (s.parametro, s.valor, s.tipo, s.descricao, true, current_timestamp(), 'seed')"""
    )
    etapas = cfg.tabela("ctl", "ctl_etapa")
    spark.createDataFrame(
        [(e, c, o, d) for e, c, o, d in ETAPAS_PADRAO],
        "etapa STRING, camada STRING, ordem INT, descricao STRING",
    ).createOrReplaceTempView("_seed_etapa")
    spark.sql(
        f"""MERGE INTO {etapas} AS t USING _seed_etapa AS s ON t.etapa = s.etapa
WHEN NOT MATCHED THEN INSERT (etapa, camada, ordem, ativo, descricao)
VALUES (s.etapa, s.camada, s.ordem, true, s.descricao)"""
    )


def setup(spark: SparkSession, cfg: Settings) -> None:
    criar_objetos(spark, cfg)
    semear_parametros(spark, cfg)


def carregar_parametros(spark: SparkSession, cfg: Settings) -> Parametros:
    linhas = spark.table(cfg.tabela("ctl", "ctl_parametro")).where("ativo = true").collect()
    valores = {r["parametro"]: (r["valor"], r["tipo"]) for r in linhas}
    return Parametros({**Parametros.padrao().valores, **valores})


def etapa_ativa(spark: SparkSession, cfg: Settings, etapa: str) -> bool:
    linhas = spark.table(cfg.tabela("ctl", "ctl_etapa")).where(f"etapa = '{etapa}'").collect()
    return True if not linhas else bool(linhas[0]["ativo"])


# --------------------------------------------------------------------------------------------------
# Cargas (maquina de estados por arquivo/data)
# --------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Carga:
    data_carga: date
    arquivo: str
    caminho: str
    status: str
    tentativas: int


def listar_cargas(spark: SparkSession, cfg: Settings, status: str) -> list[Carga]:
    """Cargas DESTA execucao no estado informado, em ordem cronologica (o SCD2 exige a ordem)."""
    rows = (
        spark.table(cfg.tabela("ctl", "ctl_carga_arquivo"))
        .where(f"run_id_ultimo = '{cfg.run_id}' AND status = '{status}'")
        .orderBy("data_carga")
        .collect()
    )
    return [
        Carga(r["data_carga"], r["arquivo"], r["caminho"], r["status"], r["tentativas"] or 0) for r in rows
    ]


def estado_cargas(spark: SparkSession, cfg: Settings) -> dict[date, Carga]:
    rows = spark.table(cfg.tabela("ctl", "ctl_carga_arquivo")).collect()
    return {
        r["data_carga"]: Carga(r["data_carga"], r["arquivo"], r["caminho"], r["status"], r["tentativas"] or 0)
        for r in rows
    }


def atualizar_carga(spark: SparkSession, cfg: Settings, data_carga: date, status: str, **campos: Any) -> None:
    """UPDATE do registro da carga. `campos` aceita linhas_bronze/linhas_silver/linhas_quarentena/mensagem."""
    permitidos = {"linhas_bronze", "linhas_silver", "linhas_quarentena", "mensagem"}
    invalidos = set(campos) - permitidos
    if invalidos:
        raise ValueError(f"campos nao permitidos em atualizar_carga: {invalidos}")
    sets = ["status = :status", "atualizado_em = current_timestamp()"]
    args: dict[str, Any] = {"status": status, "d": data_carga.isoformat()}
    for nome, valor in campos.items():
        sets.append(f"{nome} = :{nome}")
        args[nome] = valor
    spark.sql(
        f"UPDATE {cfg.tabela('ctl', 'ctl_carga_arquivo')} SET {', '.join(sets)} "
        f"WHERE data_carga = CAST(:d AS DATE)",
        args=args,
    )


def registrar_cargas(spark: SparkSession, cfg: Settings, cargas: list[dict[str, Any]]) -> None:
    """Upsert das cargas planejadas (status PENDENTE ou erro de planejamento)."""
    if not cargas:
        return
    df = spark.createDataFrame(
        [
            (
                c["data_carga"],
                c["arquivo"],
                c["caminho"],
                c["tamanho_bytes"],
                c["hash_sha256"],
                c["status"],
                c["tentativas"],
                cfg.run_id,
                c.get("mensagem"),
            )
            for c in cargas
        ],
        "data_carga DATE, arquivo STRING, caminho STRING, tamanho_bytes BIGINT, hash_sha256 STRING, "
        "status STRING, tentativas INT, run_id_ultimo STRING, mensagem STRING",
    )
    df.createOrReplaceTempView("_plano_cargas")
    spark.sql(
        f"""MERGE INTO {cfg.tabela("ctl", "ctl_carga_arquivo")} AS t USING _plano_cargas AS s
ON t.data_carga = s.data_carga
WHEN MATCHED THEN UPDATE SET t.arquivo = s.arquivo, t.caminho = s.caminho, t.tamanho_bytes = s.tamanho_bytes,
  t.hash_sha256 = s.hash_sha256, t.status = s.status, t.tentativas = s.tentativas,
  t.run_id_ultimo = s.run_id_ultimo, t.mensagem = s.mensagem, t.atualizado_em = current_timestamp()
WHEN NOT MATCHED THEN INSERT (data_carga, arquivo, caminho, tamanho_bytes, hash_sha256, status, tentativas,
  run_id_ultimo, mensagem, criado_em, atualizado_em)
VALUES (s.data_carga, s.arquivo, s.caminho, s.tamanho_bytes, s.hash_sha256, s.status, s.tentativas,
  s.run_id_ultimo, s.mensagem, current_timestamp(), current_timestamp())"""
    )


# --------------------------------------------------------------------------------------------------
# Logs
# --------------------------------------------------------------------------------------------------
def _append(spark: SparkSession, tabela: str, df: DataFrame) -> None:
    df.writeTo(tabela).append()


def registrar_erro(
    spark: SparkSession,
    cfg: Settings,
    etapa: str,
    tipo_erro: str,
    mensagem: str,
    data_carga: date | None = None,
    severidade: str = "ERRO",
    stacktrace: str | None = None,
) -> None:
    """Grava em log_erro. Nunca levanta excecao: falha de log nao pode mascarar a falha original."""
    try:
        df = spark.createDataFrame(
            [
                (
                    uuid.uuid4().hex,
                    cfg.run_id,
                    cfg.ambiente,
                    etapa,
                    data_carga,
                    severidade,
                    tipo_erro,
                    (mensagem or "")[:4000],
                    (stacktrace or "")[:20000] or None,
                    agora(),
                )
            ],
            "erro_id STRING, run_id STRING, ambiente STRING, etapa STRING, data_carga DATE, "
            "severidade STRING, tipo_erro STRING, mensagem STRING, stacktrace STRING, erro_ts TIMESTAMP",
        )
        _append(spark, cfg.tabela("ctl", "log_erro"), df)
    except Exception as exc:  # noqa: BLE001
        log.error("Falha ao gravar log_erro: %s", exc)


@dataclass
class Execucao:
    """Context manager: mede e registra uma etapa em `log_execucao` (e `log_erro` se falhar).

    with Execucao(spark, cfg, "bronze", "bronze", tabela, data_carga) as ex:
        ...
        ex.registrar(lidas=n, gravadas=n)
    """

    spark: SparkSession
    cfg: Settings
    etapa: str
    camada: str
    tabela_destino: str | None = None
    data_carga: date | None = None
    _m: dict[str, Any] = field(default_factory=dict, init=False)
    _inicio: datetime | None = field(default=None, init=False)

    def __enter__(self) -> Execucao:
        self._inicio = agora()
        log.info("[%s] inicio etapa=%s data_carga=%s", self.cfg.run_id, self.etapa, self.data_carga)
        return self

    def registrar(
        self,
        lidas: int | None = None,
        gravadas: int | None = None,
        rejeitadas: int | None = None,
        mensagem: str | None = None,
    ) -> None:
        self._m.update(lidas=lidas, gravadas=gravadas, rejeitadas=rejeitadas, mensagem=mensagem)

    def __exit__(self, exc_type, exc, tb) -> bool:
        fim = agora()
        ok = exc is None
        mensagem = self._m.get("mensagem") if ok else f"{exc_type.__name__}: {exc}"[:2000]
        try:
            df = self.spark.createDataFrame(
                [
                    (
                        self.cfg.run_id,
                        self.cfg.ambiente,
                        self.etapa,
                        self.camada,
                        self.tabela_destino,
                        self.data_carga,
                        "SUCESSO" if ok else "ERRO",
                        self._inicio,
                        fim,
                        (fim - self._inicio).total_seconds(),
                        self._m.get("lidas"),
                        self._m.get("gravadas"),
                        self._m.get("rejeitadas"),
                        mensagem,
                        _usuario(self.spark),
                        _versao_pacote(),
                    )
                ],
                "run_id STRING, ambiente STRING, etapa STRING, camada STRING, tabela_destino STRING, "
                "data_carga DATE, status STRING, inicio_ts TIMESTAMP, fim_ts TIMESTAMP, duracao_seg DOUBLE, "
                "linhas_lidas BIGINT, linhas_gravadas BIGINT, linhas_rejeitadas BIGINT, mensagem STRING, "
                "usuario STRING, versao_pacote STRING",
            )
            _append(self.spark, self.cfg.tabela("ctl", "log_execucao"), df)
        except Exception as e:  # noqa: BLE001
            log.error("Falha ao gravar log_execucao: %s", e)
        if not ok:
            registrar_erro(
                self.spark,
                self.cfg,
                self.etapa,
                exc_type.__name__,
                str(exc),
                self.data_carga,
                stacktrace="".join(traceback.format_exception(exc_type, exc, tb)),
            )
            log.error("[%s] ERRO etapa=%s: %s", self.cfg.run_id, self.etapa, exc)
        else:
            log.info("[%s] fim etapa=%s status=SUCESSO %s", self.cfg.run_id, self.etapa, self._m)
        return False  # nunca engole a excecao
