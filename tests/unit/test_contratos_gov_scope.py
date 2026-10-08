"""Escopo compartilhado das DAGs que detalham contratos."""

from unittest.mock import MagicMock

import pytest

import contratos_gov_scope
from landing_zone import RawIndisponivel

pytestmark = pytest.mark.unit


def test_escopo_padrao_e_validado(monkeypatch: pytest.MonkeyPatch) -> None:
    consultar = MagicMock(return_value=["46000", "46000"])
    monkeypatch.setattr(contratos_gov_scope.Variable, "get", consultar)

    assert contratos_gov_scope.orgaos_no_escopo() == {"46000"}
    consultar.assert_called_once_with(
        "contratos_gov_escopo_orgaos",
        default_var=["46000"],
        deserialize_json=True,
    )


def test_escopo_padrao_funciona_sem_variavel_configurada(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        contratos_gov_scope.Variable,
        "get_variable_from_secrets",
        MagicMock(return_value=None),
    )

    assert contratos_gov_scope.orgaos_no_escopo() == {"46000"}


@pytest.mark.parametrize("valor", [[], "46000", ["4600"], [46000], ["46000", None]])
def test_escopo_invalido_falha_antes_da_varredura(
    monkeypatch: pytest.MonkeyPatch, valor: object
) -> None:
    monkeypatch.setattr(
        contratos_gov_scope.Variable, "get", MagicMock(return_value=valor)
    )

    with pytest.raises(ValueError, match="contratos_gov_escopo_orgaos"):
        contratos_gov_scope.orgaos_no_escopo()


def test_ids_unem_ativos_e_inativos_e_filtram_por_orgao(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consultar = MagicMock(
        side_effect=[
            [(9, "46000"), (2, "46000"), (100, "25206")],
            [(2, "46000"), (15, "46000")],
        ]
    )
    monkeypatch.setattr(contratos_gov_scope, "distinct_raw_rows", consultar)

    assert contratos_gov_scope.ids_contratos_no_escopo({"46000"}) == [
        "2",
        "9",
        "15",
    ]
    assert consultar.call_args_list == [
        (("contratos_gov", "contrato_ativo", ["id", "orgao_codigo"]),),
        (("contratos_gov", "contrato_inativo", ["id", "orgao_codigo"]),),
    ]


@pytest.mark.parametrize("faltante", ["contrato_ativo", "contrato_inativo"])
def test_raw_ausente_indica_a_dag_necessaria(
    monkeypatch: pytest.MonkeyPatch, faltante: str
) -> None:
    def consultar(_sistema: str, entidade: str, _colunas: list[str]) -> list[tuple]:
        if entidade == faltante:
            raise RawIndisponivel(entidade)
        return [(1, "46000")]

    monkeypatch.setattr(contratos_gov_scope, "distinct_raw_rows", consultar)

    with pytest.raises(RuntimeError, match=f"{faltante}_ingest_dag"):
        contratos_gov_scope.ids_contratos_no_escopo({"46000"})


def test_sem_contrato_no_escopo_falha(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        contratos_gov_scope,
        "distinct_raw_rows",
        MagicMock(return_value=[(1, "25206")]),
    )

    with pytest.raises(RuntimeError, match="Nenhum contrato"):
        contratos_gov_scope.ids_contratos_no_escopo({"46000"})
