.PHONY: help install lint test-unit test-integration spark-submit dag-trigger terraform-init terraform-plan terraform-apply terraform-destroy

# Default target
help:
	@echo "NYC-UOIP Makefile"
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install all Python dependencies (uv)"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint             Lint Python (ruff) + SQL (sqlfluff)"
	@echo "  make test-unit        Run unit tests only"
	@echo "  make test-integration Run integration tests (requires Docker)"
	@echo ""
	@echo "Spark Jobs:"
	@echo "  make spark-submit JOB=<job-path>  Submit Spark job locally"
	@echo ""
	@echo "Airflow:"
	@echo "  make dag-trigger DAG=<dag-name>    Trigger Airflow DAG locally"
	@echo ""
	@echo "Terraform (GCP):"
	@echo "  make terraform-init               Initialize Terraform"
	@echo "  make terraform-plan               Preview Terraform changes"
	@echo "  make terraform-apply              Apply Terraform changes"
	@echo "  make terraform-destroy            Destroy GCP resources"

# ── Dependencies ──────────────────────────────────────────────────────────────

install:
	uv sync --all-extras

# ── Code Quality ───────────────────────────────────────────────────────────────

lint:
	ruff check .
	sqlfluff lint sql/ --dialect bigquery

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

# ── Spark ─────────────────────────────────────────────────────────────────────

spark-submit:
	@if [ -z "$(JOB)" ]; then \
		echo "Usage: make spark-submit JOB=spark/jobs/etl_nyc_311.py"; \
		exit 1; \
	fi
	spark-submit --master "spark://localhost:7077" $(JOB)

# ── Airflow ────────────────────────────────────────────────────────────────────

dag-trigger:
	@if [ -z "$(DAG)" ]; then \
		echo "Usage: make dag-trigger DAG=dag_ingest_nyc_311"; \
		exit 1; \
	fi
	airflow dags trigger $(DAG)

# ── Terraform ─────────────────────────────────────────────────────────────────

TERRAFORM_DIR := infra/terraform

terraform-init:
	cd $(TERRAFORM_DIR) && terraform init

terraform-plan:
	cd $(TERRAFORM_DIR) && terraform plan

terraform-apply:
	cd $(TERRAFORM_DIR) && terraform apply

terraform-destroy:
	cd $(TERRAFORM_DIR) && terraform destroy