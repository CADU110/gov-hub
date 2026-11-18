"""
Testes do cliente do Contratos.gov.br.

O que estes testes protegem são as três adaptações que o cliente faz sobre a
forma da origem, e que o resto da ingestão assume como dadas: o achatamento do
cabeçalho (o catálogo declara colunas que só existem depois dele), a injeção de
contrato_id nos sub-recursos (sem ela, publicacoes e empenhos não voltam ao
contrato; com o campo no payload, ele é conferido em vez de sobrescrito) e o
domicílio bancário, que responde objeto e sinaliza ausência com 404 — o único
endpoint em que 404 não é erro.
"""

import copy
from http import HTTPStatus
from unittest.mock import Mock, patch

import httpx
import pytest

from cliente_contratos_gov import (
    RECURSO_DOMICILIO_BANCARIO,
    ClienteContratosGov,
    ContratoNaoLocalizado,
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
    json_body: object = None,
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

    def test_contrato_id_igual_ao_da_url_e_preservado(
        self, cliente: ClienteContratosGov
    ) -> None:
        """itens traz contrato_id como int; a URL é montada com o id da raw, em
        texto. Mesmo valor em tipos diferentes não é divergência, e a raw fica
        com o valor da origem."""
        resposta = _resposta(json_body=[{"id": 31, "contrato_id": 2289}])

        with patch.object(cliente.client, "request", return_value=resposta):
            itens = cliente.listar_subrecurso("2289", "itens")

        assert itens == [{"id": 31, "contrato_id": 2289}]

    def test_contrato_id_divergente_da_url_falha(
        self, cliente: ClienteContratosGov
    ) -> None:
        """Sobrescrever apontaria o item de outro contrato para o consultado."""
        resposta = _resposta(json_body=[{"id": 31, "contrato_id": 9999}])

        with (
            patch.object(cliente.client, "request", return_value=resposta),
            pytest.raises(RuntimeError, match="contrato_id divergente"),
        ):
            cliente.listar_subrecurso(2289, "itens")

    def test_registro_sem_contrato_id_recebe_o_da_url(
        self, cliente: ClienteContratosGov
    ) -> None:
        """empenhos não traz o campo: o vínculo vem só da URL."""
        resposta = _resposta(json_body=[{"id": 7, "numero": "2024NE000001"}])

        with patch.object(cliente.client, "request", return_value=resposta):
            empenhos = cliente.listar_subrecurso("2289", "empenhos")

        assert empenhos == [{"id": 7, "numero": "2024NE000001", "contrato_id": "2289"}]

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
            pytest.raises(
                Exception, match="API request failed with a non-transient error"
            ),
        ):
            cliente.listar_subrecurso(106156, "itens")

    def test_domicilio_bancario_nao_passa_pelo_caminho_de_lista(
        self, cliente: ClienteContratosGov
    ) -> None:
        """O caminho genérico degradaria o objeto da fonte para [] em silêncio
        (_como_lista); recusar antes da chamada impede esse uso."""
        with (
            patch.object(cliente.client, "request") as requisicao,
            pytest.raises(ValueError, match="consultar_domicilio_bancario"),
        ):
            cliente.listar_subrecurso(115, RECURSO_DOMICILIO_BANCARIO)

        requisicao.assert_not_called()


# Sintético: a estrutura é a do schema `DomicilioBancario` da OpenAPI
# (https://contratos.comprasnet.gov.br/docs/api-docs.json) e a de
# ContratoController::formataOperacoesDomicilioBancario no código-fonte; os
# valores bancários são fictícios, com zeros à esquerda de propósito.
DOMICILIO_SINTETICO = {
    "unidade_gestora": "012345",
    "contrato_id": 115,
    "contrato": "00030/2023",
    "domiciliosbancarios": [
        {
            "id": 1,
            "situacao": "Registrado",
            "banco": "000",
            "agencia": "0001",
            "conta_bancaria": "0000012345",
            "codigo_operacao_credito": "0001",
            "data_registro": "31/10/2023 14:11:23",
            "updated_at": "29/11/2024 10:06:04",
        }
    ],
}


def _404(mensagem: str) -> Mock:
    return _resposta(status_code=HTTPStatus.NOT_FOUND, json_body={"error": mensagem})


class TestDomicilioBancario:
    """O único sub-recurso que responde objeto e sinaliza ausência com 404."""

    def test_objeto_e_devolvido_como_veio(self, cliente: ClienteContratosGov) -> None:
        resposta = _resposta(json_body=copy.deepcopy(DOMICILIO_SINTETICO))

        with patch.object(cliente.client, "request", return_value=resposta) as requisicao:
            domicilio = cliente.consultar_domicilio_bancario("115")

        assert requisicao.call_args.args[1] == "/api/contrato/115/domiciliobancario"
        assert domicilio == DOMICILIO_SINTETICO
        # Texto continua texto: zero à esquerda não pode virar número.
        operacao = domicilio["domiciliosbancarios"][0]
        assert (operacao["banco"], operacao["agencia"]) == ("000", "0001")
        assert operacao["conta_bancaria"] == "0000012345"
        assert operacao["codigo_operacao_credito"] == "0001"
        assert domicilio["unidade_gestora"] == "012345"

    def test_contrato_sem_domicilio_devolve_none(
        self, cliente: ClienteContratosGov
    ) -> None:
        """Mensagem conferida contra a API em 2026-11-17 (40 contratos)."""
        resposta = _404("Contrato sem domicílio bancário registrado pelo AntecipaGov.")

        with patch.object(cliente.client, "request", return_value=resposta):
            assert cliente.consultar_domicilio_bancario(115) is None

    def test_contrato_nao_localizado_tem_excecao_propria(
        self, cliente: ClienteContratosGov
    ) -> None:
        """O outro 404 da fonte não é "sem domicílio": vira exceção explícita
        para a DAG contar à parte."""
        resposta = _404("Contrato não localizado.")

        with (
            patch.object(cliente.client, "request", return_value=resposta),
            pytest.raises(ContratoNaoLocalizado, match="não localizado"),
        ):
            cliente.consultar_domicilio_bancario(999999999)

    @pytest.mark.parametrize(
        "corpo",
        [{"error": "Rota inexistente."}, {"message": "Not Found"}, ["erro"]],
        ids=["mensagem_desconhecida", "sem_error", "lista"],
    )
    def test_outro_404_falha(
        self, cliente: ClienteContratosGov, corpo: dict | list
    ) -> None:
        """404 só é ausência com a mensagem conhecida da fonte."""
        resposta = _resposta(status_code=HTTPStatus.NOT_FOUND, json_body=corpo)

        with (
            patch.object(cliente.client, "request", return_value=resposta),
            pytest.raises(RuntimeError, match="404"),
        ):
            cliente.consultar_domicilio_bancario(115)

    def test_404_sem_json_falha(self, cliente: ClienteContratosGov) -> None:
        """Página HTML de proxy ou rota errada não é "sem domicílio"."""
        resposta = _resposta(status_code=HTTPStatus.NOT_FOUND, content_type="text/html")
        resposta.text = "<html>Not Found</html>"

        with (
            patch.object(cliente.client, "request", return_value=resposta),
            pytest.raises(RuntimeError, match="404 sem o corpo"),
        ):
            cliente.consultar_domicilio_bancario(115)

    @pytest.mark.parametrize(
        "status",
        [
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.TOO_MANY_REQUESTS,
            HTTPStatus.UNAUTHORIZED,
        ],
    )
    def test_erro_http_nao_vira_vazio(
        self, cliente: ClienteContratosGov, status: HTTPStatus
    ) -> None:
        """Vazio e indisponível são coisas diferentes: o erro precisa falhar a
        task para o retry do Airflow agir, em vez de gravar uma raw incompleta."""
        resposta = _resposta(status_code=status, json_body={"error": "falha"})

        with (
            patch.object(cliente.client, "request", return_value=resposta),
            pytest.raises(RuntimeError, match=f"status {int(status)}"),
        ):
            cliente.consultar_domicilio_bancario(115)

    def test_timeout_repetido_falha_depois_das_tentativas(
        self, cliente: ClienteContratosGov
    ) -> None:
        """request_safe repete o erro de transporte e devolve 503 sintético; o
        cliente transforma isso em falha, nunca em contrato sem domicílio."""
        with (
            patch.object(
                cliente.client, "request", side_effect=httpx.ReadTimeout("timeout")
            ) as requisicao,
            patch("safe_request.time.sleep"),
            pytest.raises(RuntimeError, match="status 503"),
        ):
            cliente.consultar_domicilio_bancario(115)

        assert requisicao.call_count == ClienteContratosGov.DEFAULT_MAX_RETRIES

    @pytest.mark.parametrize(
        "corpo",
        [[copy.deepcopy(DOMICILIO_SINTETICO)], [], None, "texto"],
        ids=["lista_com_objeto", "lista_vazia", "sem_corpo", "texto"],
    )
    def test_200_fora_do_formato_documentado_falha(
        self, cliente: ClienteContratosGov, corpo: object
    ) -> None:
        """A issue esperava lista; a fonte devolve objeto. Lista, corpo vazio ou
        texto não são descartados nem reinterpretados: a task falha."""
        resposta = _resposta(json_body=corpo)
        if corpo is None:
            resposta.content = b""

        with (
            patch.object(cliente.client, "request", return_value=resposta),
            pytest.raises(RuntimeError, match="formato não reconhecido"),
        ):
            cliente.consultar_domicilio_bancario(115)

    @pytest.mark.parametrize(
        ("campos", "mensagem"),
        [
            ({"contrato_id": 9999}, "contrato_id divergente"),
            ({"contrato_id": None}, "contrato_id divergente"),
            ({"domiciliosbancarios": None}, "sem a lista domiciliosbancarios"),
            ({"domiciliosbancarios": {"id": 1}}, "sem a lista domiciliosbancarios"),
        ],
        ids=["contrato_divergente", "sem_contrato_id", "sem_lista", "lista_como_objeto"],
    )
    def test_envelope_invalido_falha(
        self, cliente: ClienteContratosGov, campos: dict, mensagem: str
    ) -> None:
        """contrato_id ausente também falha: a fonte sempre o serializa neste
        endpoint, então injetar o da URL esconderia uma mudança de formato."""
        corpo = {**copy.deepcopy(DOMICILIO_SINTETICO), **campos}
        resposta = _resposta(json_body=corpo)

        with (
            patch.object(cliente.client, "request", return_value=resposta),
            pytest.raises(RuntimeError, match=mensagem),
        ):
            cliente.consultar_domicilio_bancario(115)

    def test_contrato_id_em_texto_na_url_confere_com_inteiro_da_fonte(
        self, cliente: ClienteContratosGov
    ) -> None:
        resposta = _resposta(json_body=copy.deepcopy(DOMICILIO_SINTETICO))

        with patch.object(cliente.client, "request", return_value=resposta):
            domicilio = cliente.consultar_domicilio_bancario("115")

        assert domicilio is not None
        assert domicilio["contrato_id"] == 115
