"""
Testes do achatamento de registros do cliente Postgres.

`_flatten_data` decide o que chega à raw no backend warehouse (ADR-0021). Ele
já foi um pandas.json_normalize, que convertia inteiros em float sempre que o
lote tinha um valor faltando: a raw misturava `5` e `5.0` na mesma coluna e
gravava o texto 'NaN' no lugar de NULL. Estes testes fixam as duas coisas: o
tipo de origem chega intacto, e o resto do comportamento continua o do
json_normalize, do qual o schema das tabelas raw já existentes depende.
"""

import math

import pytest
from pandas import json_normalize

from cliente_postgres import ClientPostgresDB

pytestmark = pytest.mark.unit


@pytest.fixture
def cliente() -> ClientPostgresDB:
    # Sem __init__: o achatamento não abre conexão, e o teste não precisa de banco.
    return ClientPostgresDB.__new__(ClientPostgresDB)


class TestTiposPreservados:
    def test_inteiro_continua_inteiro_quando_o_lote_tem_nulo(
        self, cliente: ClientPostgresDB
    ) -> None:
        achatado = cliente._flatten_data([{"codigo": 5}, {"codigo": None}])

        assert achatado[0]["codigo"] == 5
        assert type(achatado[0]["codigo"]) is int

    def test_valor_nulo_vira_none_e_nao_nan(self, cliente: ClientPostgresDB) -> None:
        """NaN chegaria ao Postgres como o texto 'NaN', não como NULL."""
        achatado = cliente._flatten_data([{"codigo": 5}, {"codigo": None}])

        assert achatado[1]["codigo"] is None

    def test_coluna_ausente_no_registro_vira_none(
        self, cliente: ClientPostgresDB
    ) -> None:
        achatado = cliente._flatten_data([{"a": 1, "b": "x"}, {"a": 2}])

        assert achatado[1] == {"a": 2, "b": None}

    def test_mesmo_valor_tem_o_mesmo_tipo_com_ou_sem_nulo_no_lote(
        self, cliente: ClientPostgresDB
    ) -> None:
        """A coerção era por lote: era isso que fazia a raw misturar 5 e 5.0."""
        com_nulo = cliente._flatten_data([{"n": 5}, {"n": None}])[0]["n"]
        sem_nulo = cliente._flatten_data([{"n": 5}, {"n": 6}])[0]["n"]

        assert repr(com_nulo) == repr(sem_nulo) == "5"

    def test_float_e_bool_da_origem_sao_mantidos(self, cliente: ClientPostgresDB) -> None:
        achatado = cliente._flatten_data([{"v": 58.3, "f": True}, {"v": None, "f": None}])

        assert achatado[0] == {"v": 58.3, "f": True}
        assert not any(
            isinstance(v, float) and math.isnan(v) for v in achatado[1].values()
        )


class TestFormaDoJsonNormalize:
    def test_objeto_aninhado_une_as_chaves_pelo_separador(
        self, cliente: ClientPostgresDB
    ) -> None:
        achatado = cliente._flatten_data([{"a": 1, "b": {"c": 2, "d": {"e": 3}}}])

        assert achatado == [{"a": 1, "b__c": 2, "b__d__e": 3}]

    def test_objeto_vazio_nao_gera_coluna(self, cliente: ClientPostgresDB) -> None:
        assert cliente._flatten_data([{"a": 1, "b": {}}]) == [{"a": 1}]

    def test_lista_vira_texto(self, cliente: ClientPostgresDB) -> None:
        assert cliente._flatten_data([{"a": [1, 2]}]) == [{"a": "[1, 2]"}]

    def test_colunas_sao_a_uniao_na_ordem_em_que_aparecem(
        self, cliente: ClientPostgresDB
    ) -> None:
        """insert_data tira as colunas do primeiro registro: ele precisa ter todas."""
        achatado = cliente._flatten_data([{"b": 1}, {"a": 2, "b": 3}, {"c": 4}])

        assert [list(r) for r in achatado] == [["b", "a", "c"]] * 3

    @pytest.mark.parametrize(
        "registros",
        [
            [{"a": 1, "b": {"c": 2, "d": {"e": 3}}}],
            [{"a": 1, "b": None}, {"a": 2, "b": {"c": 3}}],
            [{"b": 1}, {"a": 2, "b": 3}, {"c": 4}],
            [{"a": {}, "x": "y"}],
        ],
    )
    def test_mesmas_colunas_do_json_normalize(
        self, cliente: ClientPostgresDB, registros: list[dict]
    ) -> None:
        """Coluna diferente da do json_normalize criaria coluna nova na raw."""
        esperado = json_normalize(registros, sep=ClientPostgresDB.SEPARATOR)

        achatado = cliente._flatten_data(registros)

        assert list(achatado[0]) == list(esperado.columns)
