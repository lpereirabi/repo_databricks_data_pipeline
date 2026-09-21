"""Gera uma carga sintetica (marketing_AAAAMMDD.csv) derivada de um arquivo base, com mutacoes deterministicas.

Serve para demonstrar/testar: SCD Tipo 2 (atributos que mudam), clientes novos, clientes que somem e a
quarentena (linhas invalidas de proposito).

    python scripts/gerar_carga_teste.py --base data/raw/marketing_20260920.csv --data 2026-09-21 --saida out/
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base", required=True)
    p.add_argument("--data", required=True, help="AAAA-MM-DD da nova carga")
    p.add_argument("--saida", default="out")
    p.add_argument("--semente", type=int, default=42)
    p.add_argument("--sem-invalidas", action="store_true", help="nao injeta linhas invalidas")
    a = p.parse_args()

    rnd = random.Random(a.semente)
    with open(a.base, encoding="utf-8-sig", newline="") as f:
        leitor = csv.DictReader(f, delimiter=";")
        colunas = leitor.fieldnames
        linhas = list(leitor)

    ids = [int(r["ID"]) for r in linhas]
    prox_id = max(ids) + 1

    # 1) mudam de salario (SCD2: nova versao)
    for r in rnd.sample(linhas, 50):
        if r["Salario Anual"].strip():
            r["Salario Anual"] = str(int(float(r["Salario Anual"]) * 1.10))
    # 2) mudam de estado civil (SCD2)
    for r in rnd.sample(linhas, 20):
        r["Estado Civil"] = rnd.choice(
            [x for x in ("Solteiro", "Casado", "Divorciado") if x != r["Estado Civil"]]
        )
    # 3) alguns clientes somem
    somem = {id(r) for r in rnd.sample(linhas, 10)}
    linhas = [r for r in linhas if id(r) not in somem]
    # 4) clientes novos (copias de perfis existentes com ID novo)
    for modelo in rnd.sample(linhas, 10):
        novo = dict(modelo)
        novo["ID"] = str(prox_id)
        prox_id += 1
        linhas.append(novo)

    if not a.sem_invalidas:
        # 5) linhas invalidas de proposito -> quarentena
        ruim = dict(linhas[0])
        ruim["Gasto com Moveis"] = "-50"  # R004 valor negativo
        dup = dict(linhas[1])  # R002 ID duplicado
        data_ruim = dict(linhas[2])
        data_ruim["ID"] = str(prox_id)
        data_ruim["Data Cadastro"] = "31/02/2022"  # R003
        id_ruim = dict(linhas[3])
        id_ruim["ID"] = "abc"  # R001
        linhas += [ruim | {"ID": str(prox_id + 1)}, dup, data_ruim, id_ruim]

    destino = Path(a.saida)
    destino.mkdir(parents=True, exist_ok=True)
    nome = destino / f"marketing_{datetime.strptime(a.data, '%Y-%m-%d'):%Y%m%d}.csv"
    with open(nome, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=colunas, delimiter=";", lineterminator="\r\n")
        w.writeheader()
        w.writerows(linhas)
    print(f"gerado: {nome} ({len(linhas)} linhas)")


if __name__ == "__main__":
    main()
