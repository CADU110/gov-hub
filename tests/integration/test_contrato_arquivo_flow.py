"""Integração da ingestão de anexos (issue #27): API, Postgres e DAG de verdade.

Roda com `make test-integration` e fica fora do CI. Fecha os critérios de aceite
que os testes unitários não alcançam:

- Postgres do compose: a DAG grava num schema descartável, nunca em
  `contratos_gov`, e a reexecução atualiza em vez de duplicar (upsert por `id`).
- API Contratos.gov.br: faz skip quando a conexão falha (há rede em que a API a
  recusa) e quando a amostra vem toda vazia, porque `[]` não confirma estrutura
  nenhuma. Os contratos saem de CONTRATOS_GOV_AMOSTRA (ids separados por
  vírgula); o padrão são contratos do órgão 17000 já usados nas medições de
  item e cronograma.
"""

import os
import sys
import uuid
from collections.abc import Callable, Generator
from pathlib import Path
from types import FunctionType
from unittest.mock import MagicMock

import httpx
import psycopg2
import psycopg2.extensions
import pytest
from airflow.models import DagBag

import landing_zone
from cliente_contratos_gov import ClienteContratosGov

pytestmark = pytest.mark.integration

RAIZ = Path(__file__).resolve().parents[2]
DAG_ID = "contrato_arquivo_ingest_dag"
DAG_FILE = RAIZ / "airflow/dags/data_ingest/contratos_gov" / f"{DAG_ID}.py"
TABELA = "raw_contrato_arquivo"

URL = "https://contratos.comprasnet.gov.br/api/contrato/{}/arquivos"
AMOSTRA = os.getenv("CONTRATOS_GOV_AMOSTRA", "167433,167445,2289").split(",")

# Contratoarquivo::arquivoAPI (gitlab.com/comprasnet/contratos).
CAMPOS = {
    "id",
    "contrato_id",
    "tipo",
    "processo",
    "sequencial_documento",
    "descricao",
    "path_arquivo",
    "origem",
    "link_sei",
}

Consulta = Callable[[str], list[tuple]]


def anexo(numero: int, **campos: object) -> dict:
    return {
        "id": numero,
        "contrato_id": 2289,
        "tipo": "Contrato",
        "processo": "00000.000000/2024-00",
        "sequencial_documento": numero,
        "descricao": f"Termo aditivo {numero:02d}",
        "path_arquivo": f"https://contratos.comprasnet.gov.br/storage/anexo-{numero}.pdf",
        "origem": "SEI",
        "link_sei": None,
        **campos,
    }


@pytest.fixture(scope="module")
def ingerir() -> FunctionType:
    dagbag = DagBag(dag_folder=str(DAG_FILE), include_examples=False)
    assert DAG_ID in dagbag.dags, f"{DAG_ID} não carregou: {dagbag.import_errors}"
    return dagbag.dags[DAG_ID].get_task("ingest_contracts").python_callable


@pytest.fixture
def consultar(
    ingerir: FunctionType, monkeypatch: pytest.MonkeyPatch
) -> Generator[Consulta, None, None]:
    """Aponta a DAG para um schema descartável e devolve como consultá-lo."""
    dsn = psycopg2.extensions.make_dsn(
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    schema = f"test_arquivo_{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("RAW_BACKEND", "warehouse")
    monkeypatch.setitem(ingerir.__globals__, "SISTEMA", schema)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", landing_zone.write_raw)
    # A connection postgres_dw só existe dentro do compose; o teste entra pelo host.
    monkeypatch.setitem(
        sys.modules, "postgres_helpers", MagicMock(get_postgres_conn=lambda _: dsn)
    )
    conexao = psycopg2.connect(dsn)
    conexao.autocommit = True

    def executar(colunas: str) -> list[tuple]:
        with conexao.cursor() as cursor:
            cursor.execute(f"SELECT {colunas} FROM {schema}.{TABELA} ORDER BY id")
            return cursor.fetchall()

    yield executar

    with conexao.cursor() as cursor:
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    conexao.close()


def usar_payload(
    ingerir: FunctionType,
    monkeypatch: pytest.MonkeyPatch,
    payload: list[dict],
) -> None:
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = lambda *_: [dict(r) for r in payload]
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)


# --- Postgres ----------------------------------------------------------------


def test_reexecucao_nao_duplica_e_atualiza(
    ingerir: FunctionType,
    consultar: Consulta,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    usar_payload(ingerir, monkeypatch, [anexo(1), anexo(2)])
    assert ingerir(["2289"])["arquivos"] == 2
    primeira = consultar("id, dt_ingest")

    usar_payload(ingerir, monkeypatch, [anexo(1, descricao="Corrigido"), anexo(2)])
    assert ingerir(["2289"])["arquivos"] == 2

    assert consultar("id, descricao") == [("1", "Corrigido"), ("2", "Termo aditivo 02")]
    segunda = consultar("id, dt_ingest")
    assert all(
        nova > antiga for (_, antiga), (_, nova) in zip(primeira, segunda, strict=True)
    )


def test_colunas_chegam_como_vieram_da_fonte(
    ingerir: FunctionType,
    consultar: Consulta,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caminho e link seguem como texto: o binário nunca é baixado."""
    usar_payload(
        ingerir,
        monkeypatch,
        [anexo(1), anexo(2, link_sei="https://sei.exemplo.gov.br/doc/2")],
    )
    ingerir(["2289"])

    assert consultar("contrato_id, path_arquivo, link_sei") == [
        ("2289", "https://contratos.comprasnet.gov.br/storage/anexo-1.pdf", None),
        (
            "2289",
            "https://contratos.comprasnet.gov.br/storage/anexo-2.pdf",
            "https://sei.exemplo.gov.br/doc/2",
        ),
    ]


# --- API ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def respostas() -> dict[str, httpx.Response]:
    """Uma chamada por contrato, sem token nenhum."""
    try:
        return {
            contrato_id: httpx.get(URL.format(contrato_id), timeout=60)
            for contrato_id in AMOSTRA
        }
    except httpx.TransportError as exc:
        pytest.skip(f"API Contratos.gov.br inacessível: {exc}")


@pytest.fixture(scope="module")
def com_dado(respostas: dict[str, httpx.Response]) -> dict[str, list[dict]]:
    achados = {
        contrato_id: resposta.json()
        for contrato_id, resposta in respostas.items()
        if resposta.status_code == httpx.codes.OK and resposta.json()
    }
    if not achados:
        pytest.skip(f"Nenhum anexo em {AMOSTRA}; defina CONTRATOS_GOV_AMOSTRA.")
    return achados


def test_fonte_responde_200_em_lista_sem_token(
    respostas: dict[str, httpx.Response],
) -> None:
    for contrato_id, resposta in respostas.items():
        assert resposta.status_code == httpx.codes.OK, contrato_id
        assert isinstance(resposta.json(), list), contrato_id


def test_estrutura_confere_com_o_codigo_fonte(com_dado: dict[str, list[dict]]) -> None:
    """Nove campos planos, e o contrato_id do payload é o da URL."""
    for contrato_id, linhas in com_dado.items():
        for linha in linhas:
            assert set(linha) == CAMPOS, contrato_id
            aninhados = [k for k, v in linha.items() if isinstance(v, (dict, list))]
            assert not aninhados, contrato_id
            assert str(linha["contrato_id"]) == contrato_id


def test_id_e_unico_na_amostra(com_dado: dict[str, list[dict]]) -> None:
    """Entre contratos também: a chave da raw é só `id`."""
    ids = [linha["id"] for linhas in com_dado.values() for linha in linhas]
    assert None not in ids
    assert len(ids) == len(set(ids))


def test_raw_confere_com_a_api_na_mesma_data(
    ingerir: FunctionType,
    consultar: Consulta,
    com_dado: dict[str, list[dict]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mesmas linhas, e a segunda execução não muda a contagem."""
    monkeypatch.setitem(
        ingerir.__globals__, "ClienteContratosGov", lambda: ClienteContratosGov(1.0)
    )
    esperado = sorted(
        (str(linha["id"]), contrato_id, linha["path_arquivo"])
        for contrato_id, linhas in com_dado.items()
        for linha in linhas
    )

    assert ingerir(list(com_dado))["arquivos"] == len(esperado)
    assert sorted(consultar("id, contrato_id, path_arquivo")) == esperado
    assert all(marca for (marca,) in consultar("dt_ingest"))

    assert ingerir(list(com_dado))["arquivos"] == len(esperado)
    assert len(consultar("id")) == len(esperado)
