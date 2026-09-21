# Operação (runbook)

Todos os comandos pressupõem `databricks auth login` feito. Troque `dev` por `stg`/`prod`.

## Rotina diária

1. Um arquivo `marketing_AAAAMMDD.csv` chega em `/Volumes/workspace/mkt_<amb>_landing/arquivos/`.
2. O job roda (agendado em prod — hoje **pausado**, ative no workspace — ou manual).
3. `data_carga = auto` processa todo arquivo ainda não carregado, em ordem cronológica.

## Consultas úteis

```sql
-- estado de cada carga
SELECT data_carga, status, tentativas, linhas_bronze, linhas_silver, linhas_quarentena, mensagem
FROM workspace.mkt_prod_ctl.ctl_carga_arquivo ORDER BY data_carga DESC;

-- o que aconteceu na última execução
SELECT etapa, data_carga, status, duracao_seg, linhas_lidas, linhas_gravadas, linhas_rejeitadas, mensagem
FROM workspace.mkt_prod_ctl.log_execucao ORDER BY inicio_ts DESC LIMIT 30;

-- erros e avisos (com stacktrace)
SELECT erro_ts, severidade, etapa, tipo_erro, mensagem, stacktrace
FROM workspace.mkt_prod_ctl.log_erro ORDER BY erro_ts DESC LIMIT 20;

-- linhas rejeitadas e por quê
SELECT * FROM workspace.mkt_prod_silver.marketing_quarentena WHERE _data_carga = DATE'2026-09-20';

-- qualidade da carga
SELECT regra, severidade, falhas, round(pct_falha, 2) pct
FROM workspace.mkt_prod_ctl.dq_resultado WHERE data_carga = DATE'2026-09-20' ORDER BY regra;
```

## Situações comuns

| Situação | O que fazer |
|---|---|
| Job falhou em uma etapa | Ver `ctl_carga_arquivo.mensagem` e `log_erro`. Corrija a causa e rode o job de novo com `data_carga=auto`: a carga em `ERRO_*` é retomada do início (até `max_tentativas`) |
| Carga travou em `ERRO_*` com tentativas esgotadas | Rode `data_carga=AAAA-MM-DD, reprocessar=true` (zera as tentativas) |
| Corrigi o CSV e quero recarregar a data | Substitua o arquivo no volume e rode `data_carga=AAAA-MM-DD, reprocessar=true` (só vale para a última carga com sucesso) |
| `ERRO_FORA_DE_ORDEM` | Chegou arquivo mais antigo que a última carga com sucesso. O SCD2 exige ordem cronológica; carregue-o antes de avançar, ou faça *full refresh* (limpe as tabelas do ambiente e recarregue em ordem) |
| Quarentena acima do limite (`LimiteRejeicaoExcedido`) | Investigue `marketing_quarentena`; se o arquivo estiver correto e o limite for o problema, ajuste `limite_rejeicao_pct` e reprocesse |
| Aviso `ARQUIVO_NOME_INVALIDO` | O arquivo não segue `marketing_AAAAMMDD.csv` e foi ignorado |
| Aviso `ARQUIVO_CONTEUDO_DUPLICADO` | O arquivo tem o mesmo hash de outra data (provável reenvio) |
| Quero desligar uma etapa | `UPDATE <ctl>.ctl_etapa SET ativo = false WHERE etapa = 'relatorio'` |
| Quero mudar uma regra/limite | `UPDATE <ctl>.ctl_parametro SET valor = '400000' WHERE parametro = 'salario_max_outlier'` — vale já na próxima execução |
| Coluna nova/faltando no CSV | Falta = a bronze falha (`SchemaInvalidoError`). Extra = aviso `COLUNAS_EXTRAS_IGNORADAS`. Para adotar a coluna, altere `schema.py`, o DDL e faça deploy |

## Scripts

| Script | Uso |
|---|---|
| `scripts/executar_job.ps1` | Deploy + execução do job com saída limpa (`-Target`, `-DataCarga`, `-Reprocessar`, `-Only`, `-SemDeploy`) |
| `scripts/verificar_carga.py` | Smoke test de dados (13 reconciliações/integridades); usado pelo estágio STG |
| `scripts/gerar_relatorio_local.py` | Gera o HTML lendo o warehouse, sem rodar o job |
| `scripts/gerar_carga_teste.py` | Gera uma carga sintética (SCD2, clientes novos, linhas inválidas) a partir de um CSV base |
| `scripts/sql_util.py` | Executa SQL no warehouse pelo terminal |
| `scripts/verificar_wheel.py` | Valida o wheel (estágio Packing) |

## Relatório HTML

O job grava `dashboard_latest.html` e `dashboard_AAAAMMDD.html` em `/Volumes/.../<prefixo>_landing/relatorios/`.
Para baixar: `databricks fs cp dbfs:/Volumes/workspace/mkt_prod_landing/relatorios/dashboard_latest.html .`
O CI também publica `relatorio-stg` / `relatorio-prod` como *artifact* da execução.

## Limites do Free Edition que afetam a operação

- Somente compute serverless, com cota diária: cada execução completa leva alguns minutos (a maior parte é a
  inicialização de cada task e a etapa de governança).
- Um SQL Warehouse pequeno; ele "dorme" e o primeiro SQL do dia demora a acordar.
- Sem criação de catálogos por API, sem `CHECK` constraints, poucos jobs simultâneos.
