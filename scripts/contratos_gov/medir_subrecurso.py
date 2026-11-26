"""
Medição de um sub-recurso por contrato do Contratos.gov.br contra a API.

Existe para fechar o critério "ingestão validada contra a API" das issues de
sub-recurso (#20 a #33) com números reproduzíveis, no formato da tabela que
docs/notas/contratos-gov-ingestao.md já usa para item e cronograma: quantos
contratos têm dado, quantas linhas, quantos `id` distintos e se algum `id`
aparece em mais de um contrato (o que derrubaria a chave primária só `id`).

Os contratos vêm de --contratos (ids separados por vírgula) ou de --ug (os
contratos ativos de uma UG, limitados por --max). O relatório traz só contagens:
nenhum valor do payload é impresso, porque há sub-recursos com dado pessoal
(terceirizados, prepostos). A exceção são os campos pedidos em --distribuicao,
que só devem ser campos categóricos sem dado pessoal (ex.: `origem`, `tipo`).

Uso:

    uv run python -m scripts.contratos_gov.medir_subrecurso arquivos --ug 170001 --max 50
    uv run python -m scripts.contratos_gov.medir_subrecurso arquivos \\
        --contratos 167433,167445 --distribuicao origem --preenchido link_sei

Deliberadamente NÃO usa o ClienteContratosGov, pelo mesmo motivo do
teste_carga: uma falha precisa aparecer no relatório em vez de ser absorvida
pelo retry.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass, field

import httpx

BASE_URL = "https://contratos.comprasnet.gov.br"
CAMINHO_SUBRECURSO = "/api/contrato/{contrato_id}/{recurso}"
CAMINHO_CONTRATOS_UG = "/api/contrato/ug/{codigo}"

TIMEOUT = 60.0
HEADERS = {"Accept-Encoding": "gzip", "Accept": "application/json"}


@dataclass
class Relatorio:
    recurso: str
    contratos: int = 0
    com_dado: int = 0
    vazios: int = 0
    falhas: dict[str, str] = field(default_factory=dict)
    linhas: int = 0
    ids_distintos: int = 0
    ids_em_mais_de_um_contrato: int = 0
    linhas_sem_id: int = 0
    contrato_id_divergente: int = 0
    maior: tuple[str, int] | None = None
    campos: list[str] = field(default_factory=list)
    distribuicao: dict[str, dict[str, int]] = field(default_factory=dict)
    preenchido: dict[str, int] = field(default_factory=dict)


def contratos_da_ug(cliente: httpx.Client, codigo: str, maximo: int) -> list[str]:
    resposta = cliente.get(CAMINHO_CONTRATOS_UG.format(codigo=codigo))
    resposta.raise_for_status()
    return [str(contrato["id"]) for contrato in resposta.json()][:maximo]


def medir(
    cliente: httpx.Client,
    recurso: str,
    contratos: list[str],
    distribuicao: list[str],
    preenchido: list[str],
    intervalo: float,
) -> Relatorio:
    relatorio = Relatorio(recurso=recurso, contratos=len(contratos))
    donos: dict[str, set[str]] = {}
    campos: set[str] = set()
    valores: dict[str, Counter] = {nome: Counter() for nome in distribuicao}
    cheios: Counter = Counter()

    for contrato_id in contratos:
        time.sleep(intervalo)
        caminho = CAMINHO_SUBRECURSO.format(contrato_id=contrato_id, recurso=recurso)
        try:
            resposta = cliente.get(caminho)
            resposta.raise_for_status()
            linhas = resposta.json()
        except (httpx.HTTPError, ValueError) as exc:
            relatorio.falhas[contrato_id] = type(exc).__name__
            continue
        if not linhas:
            relatorio.vazios += 1
            continue

        relatorio.com_dado += 1
        relatorio.linhas += len(linhas)
        if relatorio.maior is None or len(linhas) > relatorio.maior[1]:
            relatorio.maior = (contrato_id, len(linhas))
        for linha in linhas:
            campos |= set(linha)
            if linha.get("id") is None:
                relatorio.linhas_sem_id += 1
            else:
                donos.setdefault(str(linha["id"]), set()).add(contrato_id)
            if str(linha.get("contrato_id")) != contrato_id:
                relatorio.contrato_id_divergente += 1
            for nome in distribuicao:
                valores[nome][str(linha.get(nome))] += 1
            for nome in preenchido:
                if linha.get(nome) not in (None, ""):
                    cheios[nome] += 1

    relatorio.ids_distintos = len(donos)
    relatorio.ids_em_mais_de_um_contrato = sum(1 for d in donos.values() if len(d) > 1)
    relatorio.campos = sorted(campos)
    relatorio.distribuicao = {nome: dict(contagem) for nome, contagem in valores.items()}
    relatorio.preenchido = {nome: cheios[nome] for nome in preenchido}
    return relatorio


def tabela_markdown(relatorio: Relatorio) -> str:
    maior = (
        f"{relatorio.maior[1]} (contrato {relatorio.maior[0]})"
        if relatorio.maior
        else "—"
    )
    return "\n".join(
        [
            "| Contratos | Com dado | Linhas | `id` distintos "
            "| `id` em mais de um contrato | Maior |",
            "|---|---|---|---|---|---|",
            f"| {relatorio.contratos} | {relatorio.com_dado} | {relatorio.linhas} "
            f"| {relatorio.ids_distintos} | {relatorio.ids_em_mais_de_um_contrato} "
            f"| {maior} |",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("recurso", help="sufixo da rota, ex.: arquivos, terceirizados")
    origem = parser.add_mutually_exclusive_group(required=True)
    origem.add_argument("--contratos", help="ids separados por vírgula")
    origem.add_argument("--ug", help="código da UG cujos contratos ativos medir")
    parser.add_argument("--max", type=int, default=50, help="teto de contratos da UG")
    parser.add_argument(
        "--distribuicao",
        default="",
        help="campos de valor categórico a contar, separados por vírgula",
    )
    parser.add_argument(
        "--preenchido",
        default="",
        help="campos dos quais contar as linhas não nulas, separados por vírgula",
    )
    parser.add_argument("--intervalo", type=float, default=1.0, help="segundos")
    parser.add_argument("--json", action="store_true", help="relatório em JSON")
    args = parser.parse_args(argv)

    with httpx.Client(base_url=BASE_URL, headers=HEADERS, timeout=TIMEOUT) as cliente:
        try:
            contratos = (
                [c.strip() for c in args.contratos.split(",") if c.strip()]
                if args.contratos
                else contratos_da_ug(cliente, args.ug, args.max)
            )
        except httpx.HTTPError as exc:
            print(f"API inacessível ao listar contratos: {exc}", file=sys.stderr)
            return 2
        relatorio = medir(
            cliente,
            args.recurso,
            contratos,
            [c for c in args.distribuicao.split(",") if c],
            [c for c in args.preenchido.split(",") if c],
            args.intervalo,
        )

    if args.json:
        print(json.dumps(asdict(relatorio), ensure_ascii=False, indent=2))
    else:
        print(tabela_markdown(relatorio))
        print()
        print(f"Campos: {', '.join(relatorio.campos) or '—'}")
        print(f"Vazios: {relatorio.vazios}; falhas: {len(relatorio.falhas)}")
        print(
            f"Linhas sem id: {relatorio.linhas_sem_id}; "
            f"contrato_id divergente da URL: {relatorio.contrato_id_divergente}"
        )
        for nome, contagem in relatorio.distribuicao.items():
            print(f"Distribuição de {nome}: {contagem}")
        for nome, total in relatorio.preenchido.items():
            print(f"{nome} preenchido: {total} de {relatorio.linhas}")
    return 1 if relatorio.falhas else 0


if __name__ == "__main__":
    sys.exit(main())
