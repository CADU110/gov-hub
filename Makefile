export PYTHONPATH := $(CURDIR)/airflow:$(CURDIR)/airflow/helpers:$(CURDIR)/airflow/plugins

COMPOSE ?= docker compose --env-file .env -f docker/docker-compose.yml

AIRFLOW_SERVICE ?= airflow
AIRFLOW_LOCAL_DB_HOST ?= postgres
AIRFLOW_LOCAL_DB_NAME ?= postgres
AIRFLOW_LOCAL_DW_NAME ?= data_warehouse
AIRFLOW_LOCAL_DB_USER ?= postgres
AIRFLOW_LOCAL_DB_PASSWORD ?= postgres
AIRFLOW_LOCAL_DB_PORT ?= 5432
SUPERSET_ADMIN_USER ?= admin
SUPERSET_ADMIN_PASSWORD ?= admin

install:
	@echo "Instalando dependências..."
	uv sync --all-extras --group dev

requirements:
	@echo "Gerando requirements.txt (apenas dependências de runtime)..."
	uv export --no-dev --no-hashes --no-annotate --no-header --format requirements-txt -o requirements.txt

setup:
	@echo "Configurando ambiente..."
	@if ! command -v uv >/dev/null 2>&1; then \
		echo "uv não encontrado. Instale antes: https://docs.astral.sh/uv/getting-started/installation/"; \
		exit 1; \
	fi
	@if [ ! -f .env ]; then \
		if [ -f local.env ]; then \
			cp local.env .env; \
			echo ".env criado a partir de local.env"; \
		else \
			echo "local.env não encontrado. Crie o .env manualmente."; \
			exit 1; \
		fi; \
	fi
	$(MAKE) install
	$(MAKE) requirements
	bash setup-git-hooks.sh

format:
	uv run black .
	uv run ruff check --fix .
	uv run sqlfmt .

lint:
	uv run black . --check
	uv run ruff check .
	uv run ty check .
	uv run sqlfmt . --check
	$(MAKE) catalogo-validar
	$(MAKE) publicacao-validar

# --- Modelagem a partir do catálogo de sistemas estruturantes (ADR-0017) ---

# Valida o catálogo e a sincronia dos macros dbt derivados dele.
catalogo-validar:
	uv run python -m scripts.modelagem validar

# Regera os macros dbt derivados do catálogo. Rode após editar catalogo/chaves.yml.
catalogo-sync:
	uv run python -m scripts.modelagem sync

# Mapa de cruzamento: o que dá para cruzar com o quê, e por qual chave.
# Use ARGS="--mermaid" para emitir o grafo em Mermaid.
mapa:
	@uv run python -m scripts.modelagem mapa $(ARGS)

# Gera modelos dbt a partir do catálogo. Exemplos:
#   make modelo ARGS="--camada bronze --sistema compras_gov"
#   make modelo ARGS="--camada silver --sistema compras_gov --entidade contratos"
#   make modelo ARGS="--camada gold --orgao mgi --produto contratacoes \
#                     --entidade contratos_por_orgao \
#                     --cruzar compras_gov.contratos --cruzar compras_gov.uasg"
modelo:
	@uv run python -m scripts.modelagem gerar $(ARGS)

# --- Publicação: dashboards, relatórios e níveis de acesso (ADR-0019, ADR-0020) ---

# Valida o catálogo de publicação: o que os bundles expõem contra o que o
# catálogo autoriza, e o nível de acesso de cada consumidor. Roda dentro de
# `make lint` e no CI.
publicacao-validar:
	uv run python -m scripts.publicacao validar

# Regera os planos de acesso lidos pelas DAGs de publicação. Rode após editar
# catalogo/publicacao/<orgao>.yml ou catalogo/acesso.yml.
publicacao-sync:
	uv run python -m scripts.publicacao sync

# Matriz de acesso: quem enxerga qual dashboard, com que recorte de linhas.
acesso:
	@uv run python -m scripts.publicacao matriz

# Sobe o Superset local (perfil `bi` do compose) para construir e exportar
# dashboards. Fica fora do `make compose` porque é pesado e nem todo trabalho
# no framework precisa dele.
superset:
	@if [ ! -f .env ]; then cp local.env .env; echo ".env criado a partir de local.env"; fi
	$(COMPOSE) --profile bi up -d superset
	@$(COMPOSE) exec -T $(AIRFLOW_SERVICE) sh -c "printf '%s\n' '{\"superset_default\":{\"conn_type\":\"http\",\"host\":\"superset\",\"schema\":\"http\",\"port\":8088,\"login\":\"$(SUPERSET_ADMIN_USER)\",\"password\":\"$(SUPERSET_ADMIN_PASSWORD)\"}}' > /tmp/superset-connections.json && airflow connections import --overwrite /tmp/superset-connections.json && rm -f /tmp/superset-connections.json" \
		|| echo "Airflow não está em execução: crie a connection 'superset_default' antes de rodar a DAG de publicação."
	@echo "Superset em http://localhost:8088 (usuário e senha em local.env)."

test:
	uv run pytest tests/unit --junitxml=report.xml --cov=. --cov-report=xml:coverage.xml

# Os testes de integração se conectam de FORA do compose, pelo host: as portas
# vêm do .env, e não dos valores padrão, senão eles batem na porta errada em
# quem precisou remapear para conviver com outro projeto.
test-integration:
	@if [ ! -f .env ]; then cp local.env .env; echo ".env created from local.env"; fi
	@$(COMPOSE) up -d minio minio-init postgres
	@echo "Waiting for services to be healthy..."
	@$(COMPOSE) ps
	@set -a; . ./.env; set +a; \
		POSTGRES_HOST=localhost \
		POSTGRES_PORT=$${POSTGRES_HOST_PORT:-5432} \
		MINIO_ENDPOINT=http://localhost:$${MINIO_HOST_PORT:-9000} \
		uv run pytest tests/integration/ -m integration -v

compose:
	@echo "Iniciando ambiente local do Airflow com Docker Compose..."
	@if [ ! -f .env ]; then cp local.env .env; echo ".env criado a partir de local.env"; fi
	$(COMPOSE) up -d --build
	$(MAKE) dev
	$(MAKE) dev-check

dev:
	@$(COMPOSE) ps --status running $(AIRFLOW_SERVICE) >/dev/null 2>&1 || (echo "Serviço '$(AIRFLOW_SERVICE)' não está em execução. Rode: make compose" && exit 1)
	@echo "Aguardando Airflow/DB ficarem prontos..."
	@$(COMPOSE) exec -T $(AIRFLOW_SERVICE) sh -c 'for i in $$(seq 1 30); do airflow db migrate >/dev/null 2>&1 && exit 0; sleep 2; done; echo "Airflow DB não ficou pronto a tempo para inicializar."; exit 1'
	@$(COMPOSE) exec -T $(AIRFLOW_SERVICE) airflow variables set dynamic_schedules '{}'
	@$(COMPOSE) exec -T $(AIRFLOW_SERVICE) sh -c "printf '%s\n' '{\"postgres_default\":{\"conn_type\":\"postgres\",\"host\":\"$(AIRFLOW_LOCAL_DB_HOST)\",\"schema\":\"$(AIRFLOW_LOCAL_DB_NAME)\",\"login\":\"$(AIRFLOW_LOCAL_DB_USER)\",\"password\":\"$(AIRFLOW_LOCAL_DB_PASSWORD)\",\"port\":$(AIRFLOW_LOCAL_DB_PORT)},\"postgres_dw\":{\"conn_type\":\"postgres\",\"host\":\"$(AIRFLOW_LOCAL_DB_HOST)\",\"schema\":\"$(AIRFLOW_LOCAL_DW_NAME)\",\"login\":\"$(AIRFLOW_LOCAL_DB_USER)\",\"password\":\"$(AIRFLOW_LOCAL_DB_PASSWORD)\",\"port\":$(AIRFLOW_LOCAL_DB_PORT)}}' > /tmp/airflow-connections.json && airflow connections import --overwrite /tmp/airflow-connections.json && rm -f /tmp/airflow-connections.json"
	@echo "Ambiente local do Airflow configurado com sucesso."

dev-check:
	@$(COMPOSE) ps --status running $(AIRFLOW_SERVICE) >/dev/null 2>&1 || (echo "Serviço '$(AIRFLOW_SERVICE)' não está em execução. Rode: make compose" && exit 1)
	@$(COMPOSE) exec -T $(AIRFLOW_SERVICE) airflow variables get dynamic_schedules >/dev/null
	@$(COMPOSE) exec -T $(AIRFLOW_SERVICE) airflow connections get postgres_default >/dev/null
	@$(COMPOSE) exec -T $(AIRFLOW_SERVICE) airflow connections get postgres_dw >/dev/null
	@echo "Validação concluída: variables e connection do Airflow estão configuradas."

.PHONY: install requirements setup format lint test test-integration compose dev dev-check \
	catalogo-validar catalogo-sync mapa modelo publicacao-validar publicacao-sync acesso superset
