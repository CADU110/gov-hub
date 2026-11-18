"""
Cliente da API pública do Contratos.gov.br (Comprasnet Contratos).

Cobre os endpoints ABERTOS, que não exigem login. Os endpoints que filtram por
período (dt_alteracao_min/max) exigem JWT de usuário e respondem 401 sem token;
enquanto não houver credencial, não há carga incremental possível — o cabeçalho
do contrato não tem updated_at, e o único carimbo de alteração (alterado_em)
está dentro do histórico, a uma chamada por contrato.

Quatro comportamentos da origem justificam o código que existe aqui:

1. A resposta é uma lista, sem envelope e sem paginação. Não há
   `iter_pages` como no compras_gov: uma UG grande devolve tudo de uma vez (a
   maior da amostra, 1,98 MB em 18,5 s), o que motiva o timeout alto e o
   Accept-Encoding: gzip — a API comprime cerca de 7x. A exceção é o domicílio
   bancário (item 4).

2. O cabeçalho do contrato vem com três objetos aninhados. A raw precisa ser
   plana, tanto para o backend `warehouse` quanto para o catálogo declarar
   nomes de coluna que existam de fato (ADR-0017), então `achatar_contrato`
   promove os campos usados como chave conformada e descarta `links`, que só
   repete URLs deriváveis do id.

3. Sub-recurso não traz o vínculo com o contrato de forma confiável:
   `publicacoes` traz contratohistorico_id, `empenhos` não traz nenhum id de
   contrato. Por isso `listar_subrecurso` injeta contrato_id a partir da URL da
   chamada — sem isso, parte das entidades ficaria sem como voltar ao contrato.
   Quando o payload já traz o campo (`itens`), ele é conferido contra a URL em
   vez de sobrescrito: sobrescrever apontaria, em silêncio, um registro de outro
   contrato para o contrato consultado.

4. O domicílio bancário foge das duas convenções acima, e por isso tem método
   próprio (`consultar_domicilio_bancario`) em vez de passar por
   `listar_subrecurso`. A resposta de sucesso é um OBJETO, não lista, e a
   ausência de dado é 404, não 200 []. O caminho genérico degradaria o objeto
   para lista vazia (`_como_lista`) e descartaria, em silêncio, justamente o
   formato documentado.
"""

import logging
import time
from http import HTTPStatus
from typing import Any

from cliente_base import ClienteBase
from safe_request import request_safe

CAMINHO_ORGAOS = "/api/contrato/orgaos"
CAMINHO_UNIDADES = "/api/contrato/unidades"
CAMINHO_CONTRATOS_UG = "/api/contrato/ug/{codigo}"
CAMINHO_CONTRATOS_UG_INATIVOS = "/api/contrato/inativo/ug/{codigo}"
CAMINHO_SUBRECURSO = "/api/contrato/{contrato_id}/{recurso}"

# Segmento da URL do domicílio bancário. Não é aceito por listar_subrecurso:
# ver o item 4 da docstring do módulo.
RECURSO_DOMICILIO_BANCARIO = "domiciliobancario"

# Os dois 404 de ContratoController::operacoesDomicilioBancarioPorContrato
# (gitlab.com/comprasnet/contratos), conferidos contra a API em 2026-11-17. A
# comparação é por trecho, em minúsculas, para tolerar ajuste de pontuação; um
# 404 com qualquer outra mensagem, ou sem corpo JSON, é tratado como erro.
MENSAGEM_SEM_DOMICILIO = "sem domicílio bancário"
MENSAGEM_CONTRATO_NAO_LOCALIZADO = "contrato não localizado"


class ContratoNaoLocalizado(LookupError):
    """A fonte não encontrou o contrato consultado (404 "Contrato não localizado.").

    É exceção, e não lista vazia, para que nenhum chamador a confunda com
    "contrato sem domicílio": quem não a tratar explicitamente falha.
    """


# Objetos aninhados do cabeçalho, substituídos pelas colunas de achatar_contrato.
_CAMPOS_ANINHADOS = frozenset({"contratante", "fornecedor", "links"})


def achatar_contrato(registro: dict) -> dict:
    """
    Achata os objetos aninhados do cabeçalho do contrato.

    Promove a coluna que o catálogo declara como chave conformada — o código do
    órgão, o da unidade gestora e o documento do fornecedor — e descarta
    `links`. Os campos de origem (`orgao_origem`) são preservados porque
    contrato descentralizado tem UG de origem diferente da contratante.
    """
    contratante = registro.get("contratante") or {}
    orgao = contratante.get("orgao") or {}
    unidade_gestora = orgao.get("unidade_gestora") or {}
    orgao_origem = contratante.get("orgao_origem") or {}
    unidade_origem = orgao_origem.get("unidade_gestora_origem") or {}
    fornecedor = registro.get("fornecedor") or {}

    achatado = {k: v for k, v in registro.items() if k not in _CAMPOS_ANINHADOS}
    achatado.update(
        {
            "orgao_codigo": orgao.get("codigo"),
            "orgao_nome": orgao.get("nome"),
            "unidade_gestora_codigo": unidade_gestora.get("codigo"),
            "unidade_gestora_nome": unidade_gestora.get("nome"),
            "orgao_origem_codigo": orgao_origem.get("codigo"),
            "unidade_gestora_origem_codigo": unidade_origem.get("codigo"),
            "fornecedor_tipo": fornecedor.get("tipo"),
            "fornecedor_ni": fornecedor.get("cnpj_cpf_idgener"),
            "fornecedor_nome": fornecedor.get("nome"),
        }
    )
    return achatado


class ClienteContratosGov(ClienteBase):
    BASE_URL = "https://contratos.comprasnet.gov.br"

    # A maior UG da amostra levou 18,5 s numa única resposta; os 10 s herdados
    # do ClienteBase derrubariam a varredura justamente nas UGs que mais
    # importam.
    DEFAULT_TIMEOUT = 60

    REQUEST_DELAY = 1.0

    def __init__(self, request_delay: float | None = None) -> None:
        super().__init__(
            base_url=self.BASE_URL,
            headers={
                # A API comprime cerca de 7x e não anuncia gzip por conta
                # própria; sem este header a varredura por UG trafega o volume
                # inteiro.
                "Accept-Encoding": "gzip",
                "Accept": "application/json",
            },
        )
        self.request_delay = (
            self.REQUEST_DELAY if request_delay is None else request_delay
        )

    def listar_orgaos(self) -> list[dict]:
        """Órgãos com contrato registrado. Enumeração: só o código."""
        return self._listar(CAMINHO_ORGAOS)

    def listar_unidades(self) -> list[dict]:
        """Unidades gestoras com contrato registrado. Enumeração: só o código."""
        return self._listar(CAMINHO_UNIDADES)

    def listar_contratos_ug(self, codigo_unidade: str) -> list[dict]:
        """Cabeçalho dos contratos ativos de uma UG, já achatado."""
        caminho = CAMINHO_CONTRATOS_UG.format(codigo=codigo_unidade)
        return [achatar_contrato(r) for r in self._listar(caminho)]

    def listar_contratos_inativos_ug(self, codigo_unidade: str) -> list[dict]:
        """Cabeçalho dos contratos inativos de uma UG, já achatado."""
        caminho = CAMINHO_CONTRATOS_UG_INATIVOS.format(codigo=codigo_unidade)
        return [achatar_contrato(r) for r in self._listar(caminho)]

    def listar_subrecurso(self, contrato_id: Any, recurso: str) -> list[dict]:
        """
        Sub-recurso de um contrato, com contrato_id em toda linha.

        Linha sem contrato_id recebe o da URL; linha que já o traz mantém o
        valor da origem e falha se ele divergir da URL (ver `_vincular_contrato`).

        `recurso` é o segmento da URL (`historico`, `itens`, `empenhos`...),
        não o nome da entidade do catálogo. O domicílio bancário não é aceito
        aqui: use `consultar_domicilio_bancario`.
        """
        if recurso == RECURSO_DOMICILIO_BANCARIO:
            raise ValueError(
                f"'{recurso}' responde objeto, não lista, e sinaliza ausência com "
                "404: use ClienteContratosGov.consultar_domicilio_bancario."
            )
        caminho = CAMINHO_SUBRECURSO.format(contrato_id=contrato_id, recurso=recurso)
        registros = self._listar(caminho)
        return [_vincular_contrato(r, contrato_id, caminho) for r in registros]

    def consultar_domicilio_bancario(self, contrato_id: Any) -> dict | None:
        """
        Domicílio bancário de um contrato, como a fonte o entrega, ou None.

        Sucesso é um objeto (schema `DomicilioBancario` da OpenAPI):
        `{unidade_gestora, contrato_id, contrato, domiciliosbancarios: [...]}`,
        devolvido sem cópia nem alteração. Cada contrato tem no máximo um. O
        objeto precisa trazer o mesmo contrato_id da URL e a lista
        `domiciliosbancarios`; fora disso, falha, em vez de adivinhar o formato.

        Os 404 são separados pela mensagem da fonte:

        - "Contrato sem domicílio bancário registrado pelo AntecipaGov." devolve
          None: é a resposta da imensa maioria dos contratos, não erro;
        - "Contrato não localizado." levanta `ContratoNaoLocalizado`, para o
          chamador decidir e contar à parte;
        - qualquer outro 404 (rota errada, página HTML, mensagem nova) falha.

        `request_safe` é usada porque, ao contrário de `ClienteBase.request`,
        devolve o status em vez de chamar `raise_for_status()`, e só assim o 404
        se distingue de um 500. Ela repete a chamada em erro de transporte
        (timeout, conexão) e, esgotadas as tentativas, devolve 503 sintético;
        5xx não é repetido aqui. Os dois casos levantam RuntimeError, e o retry
        da task no Airflow refaz o bloco.
        """
        caminho = CAMINHO_SUBRECURSO.format(
            contrato_id=contrato_id, recurso=RECURSO_DOMICILIO_BANCARIO
        )
        time.sleep(self.request_delay)
        status, resposta = request_safe(self, "GET", caminho)
        if status == HTTPStatus.NOT_FOUND:
            return _interpretar_404_domicilio(resposta, caminho)
        if status != HTTPStatus.OK:
            raise RuntimeError(
                f"Falha ao consultar {caminho}: status {status} ({_resumo(resposta)})."
            )
        return _validar_domicilio(resposta, contrato_id, caminho)

    def _listar(self, caminho: str) -> list[dict]:
        time.sleep(self.request_delay)
        _, resposta = self.request("GET", caminho)
        return _como_lista(resposta, caminho)


def _resumo(resposta: object) -> str:
    """Trecho curto do corpo para a mensagem de erro, sem despejar payload."""
    if isinstance(resposta, dict) and isinstance(resposta.get("error"), str):
        return repr(resposta["error"][:200])
    if isinstance(resposta, (dict, list)):
        return f"corpo {type(resposta).__name__}"
    return repr(str(resposta)[:200])


def _interpretar_404_domicilio(resposta: object, caminho: str) -> None:
    mensagem = resposta.get("error") if isinstance(resposta, dict) else None
    if not isinstance(mensagem, str):
        raise RuntimeError(
            f"{caminho}: 404 sem o corpo {{'error': ...}} da API "
            f"({_resumo(resposta)}); rota ou servidor fora do esperado."
        )
    normalizada = mensagem.casefold()
    if MENSAGEM_SEM_DOMICILIO in normalizada:
        logging.info("[contratos_gov] Sem domicílio bancário em %s.", caminho)
        return None
    if MENSAGEM_CONTRATO_NAO_LOCALIZADO in normalizada:
        raise ContratoNaoLocalizado(f"{caminho}: {mensagem}")
    raise RuntimeError(f"{caminho}: 404 com mensagem desconhecida: {mensagem!r}.")


def _validar_domicilio(resposta: object, contrato_id: Any, caminho: str) -> dict:
    """Confere o envelope do domicílio bancário sem alterar o conteúdo."""
    if not isinstance(resposta, dict):
        raise RuntimeError(
            f"{caminho}: resposta {type(resposta).__name__} onde a fonte documenta "
            "o objeto DomicilioBancario; formato não reconhecido, nada foi gravado."
        )
    if str(resposta.get("contrato_id")) != str(contrato_id):
        raise RuntimeError(
            f"{caminho}: domicílio bancário com contrato_id divergente da URL "
            f"({resposta.get('contrato_id')!r} != {contrato_id!r})."
        )
    if not isinstance(resposta.get("domiciliosbancarios"), list):
        raise RuntimeError(
            f"{caminho}: domicílio bancário sem a lista domiciliosbancarios."
        )
    return resposta


def _como_lista(resposta: object, caminho: str) -> list[dict]:
    """Degrada resposta inesperada para lista vazia, como o cliente do compras_gov."""
    if not isinstance(resposta, list):
        logging.warning(
            "[contratos_gov] Resposta não é lista em %s: %r", caminho, resposta
        )
        return []
    return [r for r in resposta if isinstance(r, dict)]


def _vincular_contrato(registro: dict, contrato_id: Any, caminho: str) -> dict:
    """Injeta contrato_id da URL ou confere o que a origem já trouxe.

    A comparação é por texto porque a origem devolve int e a URL é montada com
    o id lido da raw, que pode chegar como str. O valor da origem é mantido
    quando coincide, para a raw continuar fiel à fonte; a Silver faz o cast.
    """
    if registro.get("contrato_id") is None:
        return {**registro, "contrato_id": contrato_id}
    if str(registro["contrato_id"]) != str(contrato_id):
        raise RuntimeError(
            f"{caminho}: registro com contrato_id divergente da URL "
            f"({registro['contrato_id']!r} != {contrato_id!r})."
        )
    return registro
