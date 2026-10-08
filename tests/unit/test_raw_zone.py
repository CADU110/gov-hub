"""Testes da zona raw (ADR-0021).

A promessa desta camada é uma só: a DAG de ingestão entrega o dado sem saber
qual é a forma física do destino. Os testes verificam que a mesma chamada
produz o resultado certo nos dois backends — e que o vocabulário de sistema e
entidade não muda entre eles.
"""

import json
from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import landing_zone

pytestmark = pytest.mark.unit

REGISTROS = [
    {"codigoorgao": "25206", "nomeorgao": "IPEA"},
    {"codigoorgao": "48000", "nomeorgao": "MGI"},
]


@pytest.fixture
def backend_warehouse(monkeypatch):
    monkeypatch.setenv("RAW_BACKEND", "warehouse")


@pytest.fixture
def backend_object_storage(monkeypatch):
    monkeypatch.setenv("RAW_BACKEND", "object_storage")


class TestBackendConfigurado:
    def test_padrao_e_object_storage(self, monkeypatch) -> None:
        """O padrão honra o ADR-0012; quem roda só banco declara o contrário."""
        monkeypatch.delenv("RAW_BACKEND", raising=False)
        assert landing_zone.get_raw_backend() == "object_storage"

    def test_valor_desconhecido_falha_dizendo_os_validos(self, monkeypatch) -> None:
        monkeypatch.setenv("RAW_BACKEND", "bigquery")
        with pytest.raises(ValueError) as erro:
            landing_zone.get_raw_backend()
        assert "object_storage" in str(erro.value) and "warehouse" in str(erro.value)

    def test_valor_e_normalizado(self, monkeypatch) -> None:
        monkeypatch.setenv("RAW_BACKEND", "  WAREHOUSE ")
        assert landing_zone.get_raw_backend() == "warehouse"


class TestCarimboDeIngestao:
    def test_carimba_quem_nao_tem(self) -> None:
        registros = landing_zone.stamp_ingestion([{"a": 1}, {"a": 2}])
        assert all("dt_ingest" in r for r in registros)

    def test_preserva_carimbo_existente(self) -> None:
        registros = landing_zone.stamp_ingestion([{"a": 1, "dt_ingest": "2020-01-01"}])
        assert registros[0]["dt_ingest"] == "2020-01-01"

    def test_o_lote_inteiro_recebe_o_mesmo_carimbo(self) -> None:
        registros = landing_zone.stamp_ingestion([{"a": 1}, {"a": 2}, {"a": 3}])
        assert len({r["dt_ingest"] for r in registros}) == 1


class TestEscritaNoWarehouse:
    def test_grava_na_tabela_raw_do_sistema(self, backend_warehouse) -> None:
        cliente = MagicMock()
        with patch.dict(
            "sys.modules",
            {
                "cliente_postgres": MagicMock(ClientPostgresDB=lambda _: cliente),
                "postgres_helpers": MagicMock(get_postgres_conn=lambda _: "conn"),
            },
        ):
            destino = landing_zone.write_raw(
                "compras_gov", "orgao", list(REGISTROS), primary_key=["codigoorgao"]
            )

        assert destino == "compras_gov.raw_orgao"
        _, kwargs = cliente.insert_data.call_args
        registros, tabela = cliente.insert_data.call_args[0]
        assert tabela == "raw_orgao"
        assert kwargs["schema"] == "compras_gov"
        # A chave primária vira também a chave de conflito: reexecutar a
        # ingestão atualiza a linha em vez de duplicá-la.
        assert kwargs["primary_key"] == kwargs["conflict_fields"] == ["codigoorgao"]
        assert all("dt_ingest" in r for r in registros)

    def test_campos_aninhados_opt_in_sao_json_recuperavel(
        self, backend_warehouse
    ) -> None:
        payload = [{"id": 7, "credor_obj": {"nome": "João"}, "links": ["a", "b"]}]
        cliente = MagicMock()
        with patch.dict(
            "sys.modules",
            {
                "cliente_postgres": MagicMock(ClientPostgresDB=lambda _: cliente),
                "postgres_helpers": MagicMock(get_postgres_conn=lambda _: "conn"),
            },
        ):
            landing_zone.write_raw(
                "contratos_gov",
                "contrato_empenho",
                payload,
                primary_key=["id"],
                json_fields=["credor_obj", "links"],
            )

        enviados = cliente.insert_data.call_args.args[0]
        assert json.loads(enviados[0]["credor_obj"]) == {"nome": "João"}
        assert json.loads(enviados[0]["links"]) == ["a", "b"]
        assert payload[0]["credor_obj"] == {"nome": "João"}
        assert payload[0]["links"] == ["a", "b"]


class TestEscritaEmObjectStorage:
    def test_grava_parquet_no_caminho_do_adr_0012(self, backend_object_storage) -> None:
        with (
            patch.object(landing_zone, "write_parquet") as escrever,
            patch.object(landing_zone, "get_bucket", return_value="data-lake"),
        ):
            escrever.side_effect = lambda df, caminho: caminho
            destino = landing_zone.write_raw(
                "compras_gov",
                "orgao",
                list(REGISTROS),
                run_id="manual__2026-08-25",
                run_date=date(2026, 8, 25),
            )
        assert destino is not None
        assert destino.startswith(
            "data-lake/compras_gov/orgao/2026/08/25/" "manual__2026-08-25--lote-"
        )
        assert destino.endswith(".parquet")
        dados = escrever.call_args[0][0]
        assert dados.height == 2
        assert "dt_ingest" in dados.columns

    def test_duas_escritas_na_mesma_run_geram_arquivos_distintos(
        self,
        backend_object_storage,
    ) -> None:
        with (
            patch.object(landing_zone, "write_parquet") as escrever,
            patch.object(landing_zone, "get_bucket", return_value="data-lake"),
        ):
            escrever.side_effect = lambda df, caminho: caminho

            primeiro = landing_zone.write_raw(
                "compras_gov",
                "orgao",
                list(REGISTROS),
                run_id="manual__2026-08-25",
                run_date=date(2026, 8, 25),
            )
            segundo = landing_zone.write_raw(
                "compras_gov",
                "orgao",
                list(REGISTROS),
                run_id="manual__2026-08-25",
                run_date=date(2026, 8, 25),
            )

        assert primeiro is not None
        assert segundo is not None
        assert primeiro != segundo
        assert escrever.call_count == 2

    def test_campos_aninhados_permanecem_nativos(self, backend_object_storage) -> None:
        payload = [{"id": 7, "credor_obj": {"nome": "João"}, "links": ["a", "b"]}]
        with (
            patch.object(landing_zone, "write_parquet") as escrever,
            patch.object(landing_zone, "get_bucket", return_value="data-lake"),
        ):
            escrever.side_effect = lambda df, caminho: caminho
            landing_zone.write_raw(
                "contratos_gov",
                "contrato_empenho",
                payload,
                json_fields=["credor_obj", "links"],
                run_id="teste",
            )

        dados = escrever.call_args.args[0]
        assert dados["credor_obj"].to_list() == [{"nome": "João"}]
        assert dados["links"].to_list() == [["a", "b"]]

    def test_sem_contexto_de_execucao_ainda_grava(self, backend_object_storage) -> None:
        """Fora de uma execução do Airflow o arquivo sai com run_id de horário."""
        with (
            patch.object(landing_zone, "write_parquet") as escrever,
            patch.object(landing_zone, "get_bucket", return_value="data-lake"),
        ):
            escrever.side_effect = lambda df, caminho: caminho
            destino = landing_zone.write_raw("compras_gov", "orgao", list(REGISTROS))
        assert destino is not None
        assert "sem-run-id-" in destino


class TestAssetDaRaw:
    def test_nome_identifica_sistema_e_entidade(self) -> None:
        asset = landing_zone.asset_raw("compras_gov", "arp")
        assert asset.name == "raw.compras_gov.arp"
        assert asset != landing_zone.asset_raw("compras_gov", "arp_item")

    def test_nao_depende_do_backend(self, monkeypatch) -> None:
        """Trocar o RAW_BACKEND não pode quebrar o agendamento da transformação."""
        monkeypatch.setenv("RAW_BACKEND", "warehouse")
        no_banco = landing_zone.asset_raw("compras_gov", "arp")
        monkeypatch.setenv("RAW_BACKEND", "object_storage")
        assert landing_zone.asset_raw("compras_gov", "arp") == no_banco


class TestLoteVazio:
    @pytest.mark.parametrize("backend", ["warehouse", "object_storage"])
    def test_nada_a_gravar_nao_toca_o_destino(self, backend, monkeypatch) -> None:
        monkeypatch.setenv("RAW_BACKEND", backend)
        with patch.object(landing_zone, "write_parquet") as escrever:
            assert landing_zone.write_raw("compras_gov", "orgao", []) is None
        escrever.assert_not_called()


class TestLeituraDaRaw:
    def test_valores_distintos_do_warehouse(self, backend_warehouse) -> None:
        cliente = MagicMock()
        cliente.execute_query.return_value = [("25206",), ("48000",), ("25206",)]
        with patch.dict(
            "sys.modules",
            {
                "cliente_postgres": MagicMock(ClientPostgresDB=lambda _: cliente),
                "postgres_helpers": MagicMock(get_postgres_conn=lambda _: "conn"),
            },
        ):
            valores = landing_zone.distinct_raw_values(
                "compras_gov", "orgao", "codigoorgao"
            )
        assert valores == ["25206", "48000"]
        assert "compras_gov.raw_orgao" in cliente.execute_query.call_args[0][0]

    def test_valores_distintos_do_object_storage(self, backend_object_storage) -> None:
        arquivo = "data-lake/compras_gov/orgao/2026/08/25/run.parquet"
        with (
            patch.object(landing_zone, "list_files", return_value=[arquivo]),
            patch.object(landing_zone, "get_bucket", return_value="data-lake"),
            patch.object(
                landing_zone,
                "read_parquet",
                return_value=pl.DataFrame({"codigoorgao": ["48000", "25206", "48000"]}),
            ),
        ):
            valores = landing_zone.distinct_raw_values(
                "compras_gov", "orgao", "codigoorgao"
            )
        assert valores == ["25206", "48000"]

    def test_nulos_e_vazios_ficam_de_fora(self, backend_object_storage) -> None:
        with (
            patch.object(landing_zone, "list_files", return_value=["x.parquet"]),
            patch.object(landing_zone, "get_bucket", return_value="data-lake"),
            patch.object(
                landing_zone,
                "read_parquet",
                return_value=pl.DataFrame({"codigoorgao": ["25206", None, ""]}),
            ),
        ):
            assert landing_zone.distinct_raw_values(
                "compras_gov", "orgao", "codigoorgao"
            ) == ["25206"]

    def test_entidade_ausente_diz_que_falta_ingerir(self, backend_object_storage) -> None:
        with (
            patch.object(landing_zone, "list_files", return_value=[]),
            patch.object(landing_zone, "get_bucket", return_value="data-lake"),
        ):
            with pytest.raises(landing_zone.RawIndisponivel) as erro:
                landing_zone.read_raw("compras_gov", "orgao", ["codigoorgao"])
        assert "ainda não foi ingerida" in str(erro.value)

    def test_combinacoes_distintas_vem_ordenadas(self, backend_object_storage) -> None:
        """É o `SELECT DISTINCT ... ORDER BY` de que a paginação das DAGs depende."""
        with (
            patch.object(landing_zone, "list_files", return_value=["x.parquet"]),
            patch.object(landing_zone, "get_bucket", return_value="data-lake"),
            patch.object(
                landing_zone,
                "read_parquet",
                return_value=pl.DataFrame(
                    {"ata": ["2", "1", "2", "1"], "ug": ["b", "a", "b", "b"]}
                ),
            ),
        ):
            linhas = landing_zone.distinct_raw_rows(
                "compras_gov", "arp_item", ["ata", "ug"]
            )
        assert linhas == [("1", "a"), ("1", "b"), ("2", "b")]
