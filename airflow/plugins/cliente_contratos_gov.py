"""
Cliente da API pública do Contratos.gov.br (Comprasnet Contratos).

Cobre os endpoints ABERTOS, que não exigem login. Os endpoints que filtram por
período (dt_alteracao_min/max) exigem JWT de usuário e respondem 401 sem token;
enquanto não houver credencial, não há carga incremental possível — o cabeçalho
do contrato não tem updated_at, e o único carimbo de alteração (alterado_em)
está dentro do histórico, a uma chamada por contrato.

Três comportamentos da origem justificam o código que existe aqui:

1. A resposta é sempre uma lista, sem envelope e sem paginação. Não há
   `iter_pages` como no compras_gov: uma UG grande devolve tudo de uma vez (a
   maior da amostra, 1,98 MB em 18,5 s), o que motiva o timeout alto e o
   Accept-Encoding: gzip — a API comprime cerca de 7x.

2. O cabeçalho do contrato vem com três objetos aninhados. A raw precisa ser
   plana, tanto para o backend `warehouse` quanto para o catálogo declarar
   nomes de coluna que existam de fato (ADR-0017), então `achatar_contrato`
   promove os campos usados como chave conformada e descarta `links`, que só
   repete URLs deriváveis do id.

3. Sub-recurso não traz o vínculo com o contrato de forma confiável:
   `publicacoes` traz contratohistorico_id, `empenhos` não traz nenhum id de
   contrato. Por isso `listar_subrecurso` injeta contrato_id a partir da URL da
   chamada — sem isso, parte das entidades ficaria sem como voltar ao contrato.
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

# Único sub-recurso que responde 404 {"error": ...} em vez de 200 [] quando o
# contrato não tem o dado. Tratar 404 como vazio nos demais mascararia contrato
# inexistente ou rota errada.
RECURSO_TOLERA_404 = "domiciliobancario"

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
        Sub-recurso de um contrato, com contrato_id injetado em toda linha.

        `recurso` é o segmento da URL (`historico`, `itens`, `empenhos`...),
        não o nome da entidade do catálogo.
        """
        caminho = CAMINHO_SUBRECURSO.format(contrato_id=contrato_id, recurso=recurso)
        if recurso == RECURSO_TOLERA_404:
            registros = self._listar_tolerando_404(caminho)
        else:
            registros = self._listar(caminho)
        return [{**r, "contrato_id": contrato_id} for r in registros]

    def _listar(self, caminho: str) -> list[dict]:
        time.sleep(self.request_delay)
        _, resposta = self.request("GET", caminho)
        return _como_lista(resposta, caminho)

    def _listar_tolerando_404(self, caminho: str) -> list[dict]:
        """
        Variante para o domicílio bancário, que sinaliza ausência com 404.

        `request_safe` é usada aqui porque, ao contrário de
        `ClienteBase.request`, ela não chama `raise_for_status()` — o status
        chega ao chamador em vez de virar uma exceção genérica onde o 404 seria
        indistinguível de um 500.
        """
        time.sleep(self.request_delay)
        status, resposta = request_safe(self, "GET", caminho)
        if status == HTTPStatus.NOT_FOUND:
            logging.info("[contratos_gov] Sem domicílio bancário em %s.", caminho)
            return []
        if status != HTTPStatus.OK:
            raise RuntimeError(f"Falha ao consultar {caminho}: status {status}.")
        return _como_lista(resposta, caminho)


def _como_lista(resposta: object, caminho: str) -> list[dict]:
    """Degrada resposta inesperada para lista vazia, como o cliente do compras_gov."""
    if not isinstance(resposta, list):
        logging.warning(
            "[contratos_gov] Resposta não é lista em %s: %r", caminho, resposta
        )
        return []
    return [r for r in resposta if isinstance(r, dict)]
