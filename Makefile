# ============================================================================
# Banking ETL - Makefile
# ============================================================================

.PHONY: help setup test lint format docker-build docker-up docker-down docker-logs clean

# Default target
.DEFAULT_GOAL := help

# ============================================================================
# Help
# ============================================================================
help: ## Show this help message
	@echo "Banking ETL - Available commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ============================================================================
# Setup
# ============================================================================
setup: ## Setup development environment
	@echo "Setting up development environment..."
	pip install -r requirements.txt
	cp .env.example .env
	@echo "✅ Setup complete! Edit .env with your configuration."

install: ## Install Python dependencies
	pip install -r requirements.txt

# ============================================================================
# Testing
# ============================================================================
test: ## Run all tests
	@echo "Running tests..."
	pytest tests/ -v

test-unit: ## Run unit tests only
	@echo "Running unit tests..."
	pytest tests/unit/ -v -m unit

test-integration: ## Run integration tests only
	@echo "Running integration tests..."
	pytest tests/integration/ -v -m integration

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	pytest tests/ --cov=utils --cov=spark_jobs --cov-report=html --cov-report=term-missing

# ============================================================================
# Code Quality
# ============================================================================
lint: ## Run linters
	@echo "Running linters..."
	pylint utils/ spark_jobs/
	flake8 utils/ spark_jobs/

format: ## Format code with black
	@echo "Formatting code..."
	black utils/ spark_jobs/ tests/

type-check: ## Run type checking with mypy
	@echo "Running type checks..."
	mypy utils/ spark_jobs/

quality: lint format type-check ## Run all code quality checks

# ============================================================================
# Docker Commands
# ============================================================================
docker-build: ## Build Docker images
	@echo "Building Docker images..."
	docker-compose build

docker-up: ## Start all services
	@echo "Starting services..."
	docker-compose up -d
	@echo "✅ Services started!"
	@echo "  - Spark Master UI: http://localhost:8080"
	@echo "  - Spark Worker UI: http://localhost:8081"
	@echo "  - Airflow UI: http://localhost:8082 (admin/admin)"

docker-down: ## Stop all services
	@echo "Stopping services..."
	docker-compose down

docker-down-volumes: ## Stop services and remove volumes
	@echo "Stopping services and removing volumes..."
	docker-compose down -v

docker-restart: docker-down docker-up ## Restart all services

docker-logs: ## Show logs from all services
	docker-compose logs -f

docker-logs-spark: ## Show Spark logs
	docker-compose logs -f spark-master spark-worker

docker-logs-airflow: ## Show Airflow logs
	docker-compose logs -f airflow-webserver airflow-scheduler

docker-ps: ## Show running containers
	docker-compose ps

# ============================================================================
# Development Commands
# ============================================================================
spark-shell: ## Open Spark shell
	docker-compose exec spark-master spark-shell

pyspark: ## Open PySpark shell
	docker-compose exec spark-master pyspark

spark-submit: ## Submit Spark job (use JOB=path/to/job.py)
	@if [ -z "$(JOB)" ]; then \
		echo "Error: JOB parameter is required"; \
		echo "Usage: make spark-submit JOB=spark_jobs/01_stage_to_bronze_refactored.py"; \
		exit 1; \
	fi
	docker-compose exec spark-master spark-submit /opt/spark/jobs/$(JOB)

airflow-bash: ## Open bash in Airflow container
	docker-compose exec airflow-webserver bash

# ============================================================================
# Data Commands
# ============================================================================
load-data: ## Load sample data to Hive
	@echo "Loading sample data..."
	# Add commands to load CSV data

generate-data: ## Generate test data
	python3 Generator/generate_banking_data.py
	@echo "✅ Test data generated in Data/ directory"

# ============================================================================
# Cleanup
# ============================================================================
clean: ## Clean up generated files
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	find . -type d -name "metastore_db" -exec rm -rf {} +
	rm -f derby.log
	@echo "✅ Cleanup complete"

clean-all: clean docker-down-volumes ## Clean everything including Docker volumes

# ============================================================================
# ETL Commands
# ============================================================================
etl-stage-to-bronze: ## Run Stage to Bronze ETL
	docker-compose exec spark-master \
		spark-submit --master local[*] \
		/opt/spark/jobs/01_stage_to_bronze_refactored.py

etl-bronze-to-silver: ## Run Bronze to Silver ETL
	docker-compose exec spark-master \
		spark-submit --master local[*] \
		/opt/spark/jobs/02_bronze_to_silver.py

etl-silver-to-gold: ## Run Silver to Gold ETL
	docker-compose exec spark-master \
		spark-submit --master local[*] \
		/opt/spark/jobs/03_silver_to_gold.py

etl-full: ## Run full ETL pipeline
	@echo "Running full ETL pipeline..."
	$(MAKE) etl-stage-to-bronze
	$(MAKE) etl-bronze-to-silver
	$(MAKE) etl-silver-to-gold
	@echo "✅ Full ETL pipeline completed!"

# ============================================================================
# Initialization
# ============================================================================
init-env: ## Initialize .env file from example
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ .env file created. Please edit with your configuration."; \
	else \
		echo "⚠️  .env file already exists"; \
	fi

init-airflow: ## Initialize Airflow database
	docker-compose exec airflow-webserver airflow db init
	docker-compose exec airflow-webserver airflow users create \
		--username admin \
		--password admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com

# ============================================================================
# Monitoring
# ============================================================================
monitor: ## Show resource usage
	docker stats

# ============================================================================
# Complete Setup
# ============================================================================
bootstrap: init-env docker-build docker-up init-airflow ## Complete setup from scratch
	@echo "✅ Bootstrap complete!"
	@echo "  - Edit .env file with your configuration"
	@echo "  - Access Spark UI at http://localhost:8080"
	@echo "  - Access Airflow UI at http://localhost:8082 (admin/admin)"
