"""
Teste de carga da varredura de cabeçalhos por UG do Contratos.gov.br.

Existe porque a varredura completa é a operação mais pesada da ingestão deste
sistema — 3.781 UGs, sem paginação, uma resposta inteira por UG — e a API não
publica limite de requisição: não há header de rate limit, nem ETag, e o
Cache-Control é no-cache. Sem medir, a escolha de concorrência seria chute, e o
chute errado é contra a API pública de um órgão.

Mede uma amostra das UGs em cada nível de concorrência e reporta erro, tempo e
volume. O resultado alimenta `max_active_tis_per_dag` das DAGs das issues #18 e
#19 — ver docs/notas/contratos-gov-ingestao.md.

Uso:

    uv run python -m scripts.contratos_gov.teste_carga --ugs 30 --concorrencias 1,2,4

Deliberadamente NÃO usa o ClienteContratosGov: o cliente reteta com backoff, o
que é o comportamento certo em produção e o errado aqui, onde uma falha
transitória precisa aparecer na contagem em vez de ser absorvida. As
configurações de conexão abaixo espelham as do cliente.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx

BASE_URL = "https://contratos.comprasnet.gov.br"
CAMINHO_UNIDADES = "/api/contrato/unidades"
CAMINHO_CONTRATOS_UG = "/api/contrato/ug/{codigo}"

TIMEOUT = 60.0
HEADERS = {"Accept-Encoding": "gzip", "Accept": "application/json"}


@dataclass
class Medicao:
    codigo: str
    status: int | None
    segundos: float
    bytes_recebidos: int
    contratos: int
    erro: str | None


def listar_unidades() -> list[str]:
    with httpx.Client(base_url=BASE_URL, headers=HEADERS, timeout=TIMEOUT) as cliente:
        resposta = cliente.get(CAMINHO_UNIDADES)
        resposta.raise_for_status()
        return [u["codigo"] for u in resposta.json()]


def _medir(cliente: httpx.Client, codigo: str) -> Medicao:
    inicio = time.perf_counter()
    try:
        resposta = cliente.get(CAMINHO_CONTRATOS_UG.format(codigo=codigo))
    except httpx.HTTPError as exc:
        return Medicao(
            codigo=codigo,
            status=None,
            segundos=time.perf_counter() - inicio,
            bytes_recebidos=0,
            contratos=0,
            erro=f"{type(exc).__name__}: {exc}",
        )

    segundos = time.perf_counter() - inicio
    contratos = 0
    if resposta.status_code == httpx.codes.OK:
        corpo = resposta.json()
        contratos = len(corpo) if isinstance(corpo, list) else 0

    return Medicao(
        codigo=codigo,
        status=resposta.status_code,
        segundos=segundos,
        # Tamanho já descomprimido: o httpx desfaz o gzip antes de expor o
        # corpo. O que trafega na rede é cerca de 7x menor.
        bytes_recebidos=len(resposta.content),
        contratos=contratos,
        erro=None if resposta.status_code == httpx.codes.OK else "status inesperado",
    )


def rodar(codigos: list[str], concorrencia: int) -> list[Medicao]:
    with httpx.Client(base_url=BASE_URL, headers=HEADERS, timeout=TIMEOUT) as cliente:
        if concorrencia == 1:
            return [_medir(cliente, codigo) for codigo in codigos]
        with ThreadPoolExecutor(max_workers=concorrencia) as executor:
            return list(executor.map(lambda codigo: _medir(cliente, codigo), codigos))


def _percentil(valores: list[float], fracao: float) -> float:
    ordenados = sorted(valores)
    indice = min(int(fracao * len(ordenados)), len(ordenados) - 1)
    return ordenados[indice]


def resumir(concorrencia: int, medicoes: list[Medicao], segundos_totais: float) -> dict:
    tempos = [m.segundos for m in medicoes]
    erros = [m for m in medicoes if m.erro]
    return {
        "concorrencia": concorrencia,
        "ugs": len(medicoes),
        "erros": len(erros),
        "segundos_totais": round(segundos_totais, 1),
        "ug_por_segundo": round(len(medicoes) / segundos_totais, 2),
        "segundos_mediana": round(statistics.median(tempos), 2),
        "segundos_p95": round(_percentil(tempos, 0.95), 2),
        "segundos_max": round(max(tempos), 2),
        "mb_total": round(sum(m.bytes_recebidos for m in medicoes) / 1024 / 1024, 1),
        "contratos": sum(m.contratos for m in medicoes),
        "amostra_de_erros": [m.erro for m in erros[:3]],
    }


def _imprimir(resumos: list[dict]) -> None:
    cabecalho = (
        f"{'conc':>4} {'UGs':>5} {'erros':>6} {'total(s)':>9} "
        f"{'UG/s':>6} {'p50(s)':>7} {'p95(s)':>7} {'max(s)':>7} {'MB':>7}"
    )
    print(cabecalho)
    print("-" * len(cabecalho))
    for r in resumos:
        print(
            f"{r['concorrencia']:>4} {r['ugs']:>5} {r['erros']:>6} "
            f"{r['segundos_totais']:>9} {r['ug_por_segundo']:>6} "
            f"{r['segundos_mediana']:>7} {r['segundos_p95']:>7} "
            f"{r['segundos_max']:>7} {r['mb_total']:>7}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ugs", type=int, default=30, help="quantas UGs amostrar (padrão: 30)"
    )
    parser.add_argument(
        "--concorrencias",
        default="1,2,4",
        help="níveis de concorrência separados por vírgula (padrão: 1,2,4)",
    )
    parser.add_argument(
        "--semente",
        type=int,
        default=42,
        help="semente da amostragem, para a mesma amostra em todos os níveis",
    )
    parser.add_argument(
        "--saida", type=Path, help="arquivo JSON com as medições individuais"
    )
    args = parser.parse_args(argv)

    concorrencias = [int(c) for c in args.concorrencias.split(",")]

    unidades = listar_unidades()
    print(f"UGs disponíveis: {len(unidades)}")

    # A mesma amostra em todos os níveis: comparar concorrências sobre UGs
    # diferentes mediria o tamanho das UGs sorteadas, não a concorrência.
    amostra = random.Random(args.semente).sample(unidades, min(args.ugs, len(unidades)))
    print(f"amostra: {len(amostra)} UGs (semente {args.semente})\n")

    resumos = []
    detalhes = {}
    for concorrencia in concorrencias:
        print(f"rodando concorrência {concorrencia}...", file=sys.stderr)
        inicio = time.perf_counter()
        medicoes = rodar(amostra, concorrencia)
        resumos.append(resumir(concorrencia, medicoes, time.perf_counter() - inicio))
        detalhes[str(concorrencia)] = [asdict(m) for m in medicoes]

    _imprimir(resumos)

    if args.saida:
        args.saida.write_text(
            json.dumps({"resumo": resumos, "medicoes": detalhes}, indent=2),
            encoding="utf-8",
        )
        print(f"\nmedições gravadas em {args.saida}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
