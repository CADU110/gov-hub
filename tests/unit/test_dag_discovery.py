import os
from pathlib import Path

import pytest

import dag_discovery
from dag_discovery import DagSelector

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_selector(tmp_path: Path, content: str | None) -> DagSelector:
    if content is not None:
        (tmp_path / "dag_selector").write_text(content, encoding="utf-8")
    return DagSelector(dags_folder=tmp_path)


# ---------------------------------------------------------------------------
# DagSelector.is_included
# ---------------------------------------------------------------------------


class TestDagSelectorIsIncluded:
    def test_missing_file_includes_everything(self, tmp_path: Path) -> None:
        selector = _make_selector(tmp_path, content=None)

        assert selector.is_included(tmp_path / "data_ingest" / "anything.py")

    def test_wildcard_marker_includes_everything(self, tmp_path: Path) -> None:
        selector = _make_selector(tmp_path, content="*\n")

        assert selector.is_included(tmp_path / "data_ingest" / "anything.py")

    def test_comments_and_blank_lines_are_ignored(self, tmp_path: Path) -> None:
        content = "\n# comment\n\ndata_ingest/ibge/\n"
        selector = _make_selector(tmp_path, content=content)

        assert selector.is_included(tmp_path / "data_ingest" / "ibge" / "dag.py")
        assert not selector.is_included(tmp_path / "data_ingest" / "cnpq" / "dag.py")

    def test_included_folder_does_not_propagate_to_subfolder(
        self, tmp_path: Path
    ) -> None:
        content = "data_ingest/compras_gov/\n"
        selector = _make_selector(tmp_path, content=content)

        assert selector.is_included(tmp_path / "data_ingest" / "compras_gov" / "dag.py")
        assert not selector.is_included(
            tmp_path / "data_ingest" / "compras_gov" / "mir" / "dag.py"
        )

    def test_explicitly_declared_subfolder_is_included(self, tmp_path: Path) -> None:
        content = "data_ingest/compras_gov/mir/\n"
        selector = _make_selector(tmp_path, content=content)

        assert selector.is_included(
            tmp_path / "data_ingest" / "compras_gov" / "mir" / "dag.py"
        )
        assert not selector.is_included(
            tmp_path / "data_ingest" / "compras_gov" / "other_org" / "dag.py"
        )

    def test_file_outside_dags_folder_is_not_filtered(self, tmp_path: Path) -> None:
        content = "data_ingest/ibge/\n"
        selector = _make_selector(tmp_path, content=content)

        outside_path = tmp_path.parent / "outside_dags" / "file.py"
        assert selector.is_included(outside_path)

    def test_reloads_after_file_change(self, tmp_path: Path) -> None:
        selector_file = tmp_path / "dag_selector"
        selector_file.write_text("data_ingest/ibge/\n", encoding="utf-8")
        selector = DagSelector(dags_folder=tmp_path)

        assert not selector.is_included(tmp_path / "data_ingest" / "cnpq" / "dag.py")

        selector_file.write_text("data_ingest/cnpq/\n", encoding="utf-8")
        # Duas escritas seguidas podem cair no mesmo tick do mtime; força um novo.
        stat = selector_file.stat()
        os.utime(selector_file, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

        assert selector.is_included(tmp_path / "data_ingest" / "cnpq" / "dag.py")
        assert not selector.is_included(tmp_path / "data_ingest" / "ibge" / "dag.py")


# ---------------------------------------------------------------------------
# might_contain_selected_dag — o callable que o Airflow realmente chama
# ---------------------------------------------------------------------------


class TestCallableDoAirflow:
    """Exercita a função configurada em `core.might_contain_dag_callable`.

    Testar apenas `DagSelector.is_included` deixou passar uma recursão infinita
    entre este callable e a heurística padrão do Airflow: o seletor estava
    correto, e mesmo assim nenhuma DAG era carregada.
    """

    def _arquivo_de_dag(self, tmp_path: Path, pasta: str) -> Path:
        destino = tmp_path / pasta
        destino.mkdir(parents=True, exist_ok=True)
        arquivo = destino / "exemplo_ingest_dag.py"
        arquivo.write_text("from airflow.sdk import DAG\n", encoding="utf-8")
        return arquivo

    def _com_seletor(self, monkeypatch, tmp_path: Path, conteudo: str) -> None:
        (tmp_path / "dag_selector").write_text(conteudo, encoding="utf-8")
        monkeypatch.setattr(
            dag_discovery, "_dag_selector", DagSelector(dags_folder=tmp_path)
        )

    def test_arquivo_na_allowlist_e_aceito(self, tmp_path: Path, monkeypatch) -> None:
        arquivo = self._arquivo_de_dag(tmp_path, "data_ingest/compras_gov")
        self._com_seletor(monkeypatch, tmp_path, "data_ingest/compras_gov/\n")
        assert dag_discovery.might_contain_selected_dag(str(arquivo)) is True

    def test_arquivo_fora_da_allowlist_e_recusado(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        arquivo = self._arquivo_de_dag(tmp_path, "data_ingest/outro_sistema")
        self._com_seletor(monkeypatch, tmp_path, "data_ingest/compras_gov/\n")
        assert dag_discovery.might_contain_selected_dag(str(arquivo)) is False

    def test_arquivo_sem_marca_de_dag_e_recusado(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A heurística padrão do Airflow continua valendo antes do seletor."""
        pasta = tmp_path / "data_ingest/compras_gov"
        pasta.mkdir(parents=True)
        arquivo = pasta / "helper.py"
        arquivo.write_text("VALOR = 1\n", encoding="utf-8")
        self._com_seletor(monkeypatch, tmp_path, "data_ingest/compras_gov/\n")
        assert dag_discovery.might_contain_selected_dag(str(arquivo)) is False
