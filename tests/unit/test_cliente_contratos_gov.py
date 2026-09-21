"""
Testes do cliente do Contratos.gov.br.

O que estes testes protegem são as três adaptações que o cliente faz sobre a
forma da origem, e que o resto da ingestão assume como dadas: o achatamento do
cabeçalho (o catálogo declara colunas que só existem depois dele), a injeção de
contrato_id nos sub-recursos (sem ela, publicacoes e empenhos não voltam ao
contrato) e o 404 do domicílio bancário tratado como vazio — e só nele.
"""

from http import HTTPStatus
from unittest.mock import Mock, patch

import httpx
import pytest

from cliente_contratos_gov import (
    RECURSO_TOLERA_404,
    ClienteContratosGov,
    achatar_contrato,
)

pytestmark = pytest.mark.unit


CONTRATO_ANINHADO = {
    "id": 106156,
    "numero": "00001/2021",
    "situacao": "Ativo",
    "valor_inicial": "58.314,00",
    "contratante": {
        "orgao_origem": {
            "codigo": "03000",
            "nome": "TRIBUNAL DE CONTAS DA UNIAO",
            "unidade_gestora_origem": {"codigo": "030001", "nome_resumido": "TCU"},
        },
        "orgao": {
            "codigo": "03000",
            "nome": "TRIBUNAL DE CONTAS DA UNIAO",
            "unidade_gestora": {
                "codigo": "030001",
                "nome": "TRIBUNAL DE CONTAS DA UNIAO",
            },
        },
    },
    "fornecedor": {
        "tipo": "JURIDICA",
        "cnpj_cpf_idgener": "09.132.659/0001-76",
        "nome": "EMBRATEL TVSAT TELECOMUNICACOES SA",
    },
    "links": {"historico": "https://exemplo/api/contrato/106156/historico"},
}


@pytest.fixture
def cliente() -> ClienteContratosGov:
    # Sem espera entre chamadas: o intervalo é contra a API pública, não contra
    # o mock, e deixaria a suíte um segundo mais lenta por requisição.
    return ClienteContratosGov(request_delay=0)


def _resposta(
    status_code: HTTPStatus = HTTPStatus.OK,
    json_body: dict | list | None = None,
    content_type: str = "application/json",
) -> Mock:
    resposta = Mock()
    resposta.status_code = status_code
    resposta.json.return_value = json_body
    resposta.raise_for_status.return_value = None
    resposta.content = b"[]"
    resposta.headers = {"Content-Type": content_type}
    return resposta


class TestAchatamentoDoCabecalho:
    def test_promove_colunas_declaradas_no_catalogo(self) -> None:
        achatado = achatar_contrato(CONTRATO_ANINHADO)

        assert achatado["orgao_codigo"] == "03000"
        assert achatado["unidade_gestora_codigo"] == "030001"
        assert achatado["fornecedor_ni"] == "09.132.659/0001-76"
        assert achatado["fornecedor_tipo"] == "JURIDICA"
        assert achatado["orgao_origem_codigo"] == "03000"
        assert achatado["unidade_gestora_origem_codigo"] == "030001"

    def test_remove_objetos_aninhados(self) -> None:
        achatado = achatar_contrato(CONTRATO_ANINHADO)

        assert "contratante" not in achatado
        assert "fornecedor" not in achatado
        assert "links" not in achatado

    def test_preserva_campos_escalares(self) -> None:
        achatado = achatar_contrato(CONTRATO_ANINHADO)

        assert achatado["id"] == 106156
        assert achatado["numero"] == "00001/2021"
        assert achatado["valor_inicial"] == "58.314,00"

    def test_tolera_aninhamento_ausente(self) -> None:
        """Contrato sem contratante/fornecedor não pode derrubar a varredura da UG."""
        achatado = achatar_contrato({"id": 1, "contratante": None})

        assert achatado["id"] == 1
        assert achatado["orgao_codigo"] is None
        assert achatado["fornecedor_ni"] is None


class TestEnumeracoes:
    def test_listar_orgaos_devolve_a_lista(self, cliente: ClienteContratosGov) -> None:
        resposta = _resposta(json_body=[{"codigo": "02000"}, {"codigo": "03000"}])

        with patch.object(cliente.client, "request", return_value=resposta):
            orgaos = cliente.listar_orgaos()

        assert orgaos == [{"codigo": "02000"}, {"codigo": "03000"}]

    def test_resposta_fora_do_formato_degrada_para_vazio(
        self, cliente: ClienteContratosGov
    ) -> None:
        """A origem é PHP sem contrato de erro estável; um dict no lugar da
        lista não pode virar exceção no meio de uma varredura de 3.781 UGs."""
        resposta = _resposta(json_body={"error": "erro inesperado"})

        with patch.object(cliente.client, "request", return_value=resposta):
            assert cliente.listar_unidades() == []

    def test_envia_accept_encoding_gzip(self, cliente: ClienteContratosGov) -> None:
        assert cliente.client.headers["accept-encoding"] == "gzip"


class TestCabecalhoPorUnidade:
    def test_contratos_ativos_vem_achatados(self, cliente: ClienteContratosGov) -> None:
        resposta = _resposta(json_body=[CONTRATO_ANINHADO])

        with patch.object(cliente.client, "request", return_value=resposta) as requisicao:
            contratos = cliente.listar_contratos_ug("030001")

        assert requisicao.call_args.args[1] == "/api/contrato/ug/030001"
        assert contratos[0]["unidade_gestora_codigo"] == "030001"
        assert "contratante" not in contratos[0]

    def test_contratos_inativos_usam_a_rota_de_inativos(
        self, cliente: ClienteContratosGov
    ) -> None:
        resposta = _resposta(json_body=[CONTRATO_ANINHADO])

        with patch.object(cliente.client, "request", return_value=resposta) as requisicao:
            cliente.listar_contratos_inativos_ug("030001")

        assert requisicao.call_args.args[1] == "/api/contrato/inativo/ug/030001"


class TestSubrecursos:
    def test_injeta_contrato_id_em_toda_linha(self, cliente: ClienteContratosGov) -> None:
        """publicacoes traz contratohistorico_id e empenhos não traz id nenhum;
        o vínculo com o contrato só existe porque vem da URL da chamada."""
        resposta = _resposta(json_body=[{"id": 1, "contratohistorico_id": 9}, {"id": 2}])

        with patch.object(cliente.client, "request", return_value=resposta):
            publicacoes = cliente.listar_subrecurso(106156, "publicacoes")

        assert [p["contrato_id"] for p in publicacoes] == [106156, 106156]
        assert publicacoes[0]["contratohistorico_id"] == 9

    def test_404_no_domicilio_bancario_vira_lista_vazia(
        self, cliente: ClienteContratosGov
    ) -> None:
        resposta = _resposta(
            status_code=HTTPStatus.NOT_FOUND, json_body={"error": "não encontrado"}
        )

        with patch.object(cliente.client, "request", return_value=resposta):
            assert cliente.listar_subrecurso(106156, RECURSO_TOLERA_404) == []

    def test_404_em_outro_recurso_nao_e_tolerado(
        self, cliente: ClienteContratosGov
    ) -> None:
        """Só o domicílio bancário sinaliza ausência com 404. Tolerar nos demais
        esconderia contrato inexistente ou rota errada."""
        resposta = _resposta(status_code=HTTPStatus.NOT_FOUND)
        resposta.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=Mock(), response=resposta
        )

        with (
            patch.object(cliente.client, "request", return_value=resposta),
            patch("cliente_base.time.sleep"),
            pytest.raises(Exception, match="API failed after the maximum"),
        ):
            cliente.listar_subrecurso(106156, "itens")

    def test_erro_no_domicilio_bancario_nao_vira_vazio(
        self, cliente: ClienteContratosGov
    ) -> None:
        """Vazio e indisponível são coisas diferentes: 500 precisa falhar a task
        para o retry do Airflow agir, em vez de gravar uma raw incompleta."""
        resposta = _resposta(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)

        with (
            patch.object(cliente.client, "request", return_value=resposta),
            pytest.raises(RuntimeError, match="Falha ao consultar"),
        ):
            cliente.listar_subrecurso(106156, RECURSO_TOLERA_404)
