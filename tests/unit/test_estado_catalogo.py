"""Testes do estado atual do catálogo na raw.

A DAG base de catálogo só dispara a de inativos quando a conta daqui diverge do
total da API. Contar linhas, e não o último estado de cada código, faria a raw
append-only parecer sempre desatualizada ou sempre em dia.
"""

import polars as pl
import pytest

import estado_catalogo

pytestmark = pytest.mark.unit


def _raw(linhas: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        linhas, schema=["codigoPdm", "statusPdm", "dt_ingest"], orient="row"
    )


def _contar(dados: pl.DataFrame) -> int:
    return estado_catalogo.contar_inativos(dados, "codigoPdm", "statusPdm")


class TestContarInativos:
    def test_so_ativos_da_zero(self) -> None:
        dados = _raw([(1, True, "2026-10-20T02:00:00"), (2, True, "2026-10-20T02:00:00")])
        assert _contar(dados) == 0

    def test_conta_codigos_inativos(self) -> None:
        dados = _raw(
            [
                (1, True, "2026-10-20T02:00:00"),
                (2, False, "2026-10-20T02:30:00"),
                (3, False, "2026-10-20T02:30:00"),
            ]
        )
        assert _contar(dados) == 2

    def test_ativo_que_foi_inativado_conta(self) -> None:
        dados = _raw(
            [(1, True, "2026-10-20T02:00:00"), (1, False, "2026-10-27T02:30:00")]
        )
        assert _contar(dados) == 1

    def test_inativo_que_foi_reativado_nao_conta(self) -> None:
        dados = _raw(
            [(1, False, "2026-10-20T02:30:00"), (1, True, "2026-10-27T02:00:00")]
        )
        assert _contar(dados) == 0

    def test_ordem_das_linhas_nao_importa(self) -> None:
        # A raw concatena os Parquets na ordem do glob, não na de ingestão.
        dados = _raw(
            [(1, True, "2026-10-27T02:00:00"), (1, False, "2026-10-20T02:30:00")]
        )
        assert _contar(dados) == 0

    def test_inativo_repetido_conta_uma_vez(self) -> None:
        dados = _raw(
            [(1, False, "2026-10-20T02:30:00"), (1, False, "2026-10-27T02:30:00")]
        )
        assert _contar(dados) == 1

    @pytest.mark.parametrize("ativo, inativo", [("true", "false"), ("True", "False")])
    def test_status_em_texto_do_warehouse(self, ativo: str, inativo: str) -> None:
        dados = _raw(
            [
                (1, ativo, "2026-10-20T02:00:00"),
                (2, inativo, "2026-10-20T02:30:00"),
            ]
        )
        assert _contar(dados) == 1
