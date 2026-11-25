"""Testes do envio de relatório por e-mail (ADR-0019).

O ponto sensível aqui não é o SMTP: é a lista de destinatários. Um relatório
entregue a vários órgãos não pode expor, para cada um, o endereço de todos os
outros — endereço de pessoa é dado pessoal (ADR-0013).
"""

from email.message import EmailMessage

import pytest

import cliente_email

pytestmark = pytest.mark.unit

CREDENCIAIS = {
    "host": "smtp.exemplo.gov.br",
    "port": 587,
    "usuario": "govhub",
    "senha": "segredo",
    "remetente": "govhub@exemplo.gov.br",
}


class SmtpFalso:
    """Servidor SMTP de mentira que guarda o que foi feito com ele."""

    instancias: list["SmtpFalso"] = []

    def __init__(self, host, porta, timeout=None) -> None:
        self.host = host
        self.porta = porta
        self.timeout = timeout
        self.tls = False
        self.login_com: tuple[str, str] | None = None
        self.mensagem: EmailMessage | None = None
        SmtpFalso.instancias.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def starttls(self):
        self.tls = True

    def login(self, usuario, senha):
        self.login_com = (usuario, senha)

    def send_message(self, mensagem):
        self.mensagem = mensagem


@pytest.fixture
def smtp(monkeypatch):
    SmtpFalso.instancias = []
    monkeypatch.setattr(cliente_email.smtplib, "SMTP", SmtpFalso)
    return SmtpFalso


class TestEnvioDeRelatorio:
    def _enviar(self, destinatarios, anexos=None):
        cliente_email.enviar_email(
            credenciais=CREDENCIAIS,
            destinatarios=destinatarios,
            assunto="Contratações do mês — 03/2026",
            corpo="Segue o extrato.",
            anexos=anexos or [("contratacoes_ipea_2026-03-01.csv", b"co_orgao\n25206\n")],
        )

    def test_destinatarios_vao_em_copia_oculta(self, smtp) -> None:
        self._enviar(["a@ipea.gov.br", "b@ipea.gov.br"])
        mensagem = smtp.instancias[0].mensagem
        assert mensagem["Bcc"] == "a@ipea.gov.br, b@ipea.gov.br"
        # O cabeçalho visível é o próprio remetente: ninguém recebe a lista.
        assert mensagem["To"] == CREDENCIAIS["remetente"]

    def test_anexo_preserva_nome_e_conteudo(self, smtp) -> None:
        self._enviar(["a@ipea.gov.br"])
        anexos = list(smtp.instancias[0].mensagem.iter_attachments())
        assert len(anexos) == 1
        assert anexos[0].get_filename() == "contratacoes_ipea_2026-03-01.csv"
        assert anexos[0].get_content().strip() == "co_orgao\n25206"

    def test_tipo_do_anexo_vem_da_extensao(self, smtp) -> None:
        self._enviar(["a@ipea.gov.br"], anexos=[("relatorio.csv", b"a,b\n1,2\n")])
        anexo = next(smtp.instancias[0].mensagem.iter_attachments())
        assert anexo.get_content_type() == "text/csv"

    def test_extensao_desconhecida_vira_binario(self, smtp) -> None:
        # Extensão inventada de propósito: guess_type consulta o banco de tipos do
        # sistema, e uma extensão real (.parquet) passou a ser conhecida na imagem
        # do GitHub Actions em 2026-10, quebrando o teste sem mudança no código.
        self._enviar(["a@ipea.gov.br"], anexos=[("relatorio.desconhecida", b"PAR1")])
        anexo = next(smtp.instancias[0].mensagem.iter_attachments())
        assert anexo.get_content_type() == "application/octet-stream"

    def test_usa_tls_e_autentica(self, smtp) -> None:
        self._enviar(["a@ipea.gov.br"])
        servidor = smtp.instancias[0]
        assert servidor.tls is True
        assert servidor.login_com == ("govhub", "segredo")
        assert (servidor.host, servidor.porta) == ("smtp.exemplo.gov.br", 587)

    def test_tls_pode_ser_desligado(self, smtp) -> None:
        cliente_email.enviar_email(
            credenciais={**CREDENCIAIS, "tls": False},
            destinatarios=["a@ipea.gov.br"],
            assunto="x",
            corpo="y",
        )
        assert smtp.instancias[0].tls is False

    def test_sem_destinatario_nao_abre_conexao(self, smtp) -> None:
        self._enviar([])
        assert smtp.instancias == []

    def test_sem_remetente_falha_antes_de_conectar(self, smtp) -> None:
        with pytest.raises(ValueError):
            cliente_email.enviar_email(
                credenciais={"host": "smtp.exemplo.gov.br"},
                destinatarios=["a@ipea.gov.br"],
                assunto="x",
                corpo="y",
            )
        assert smtp.instancias == []
