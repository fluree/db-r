# OTEL Testing & Validation Infrastructure
# See README.md for usage docs.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT_DIR   := $(shell cd .. && pwd)
DATA_DIR   := $(CURDIR)/_data
STORAGE    := $(DATA_DIR)/storage
GEN_DIR    := $(DATA_DIR)/generated
PID_FILE   := $(DATA_DIR)/server.pid
LOG_FILE   := $(DATA_DIR)/server.log
SERVER_BIN := $(ROOT_DIR)/target/release/fluree-server
INGEST_BIN := $(ROOT_DIR)/target/release/fluree-ingest

# ── Configurable ─────────────────────────────────────────────────────────────
PORT       ?= 8090
BASE_URL   ?= http://localhost:$(PORT)
LEDGER     ?= otel-test:main
ENTITIES   ?= 100000
PROPS      ?= 6
INDEXING   ?= true
PARSE_THREADS ?= 4

STRESS_PRODUCTS ?= 50000
STRESS_BATCH    ?= 500

RUST_LOG   ?= info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug

# ── Infrastructure lifecycle ─────────────────────────────────────────────────

.PHONY: up down reset ui

up: ## Start Jaeger via docker compose
	docker compose up -d
	@echo "Jaeger UI: http://localhost:16686"

down: ## Stop and remove Jaeger container
	docker compose down

reset: ## Restart Jaeger (clears all trace data)
	docker compose down
	docker compose up -d
	@echo "Jaeger restarted — trace data cleared"

ui: ## Open Jaeger UI in browser
	@open http://localhost:16686 2>/dev/null || xdg-open http://localhost:16686 2>/dev/null || echo "Open http://localhost:16686"

# ── Build ────────────────────────────────────────────────────────────────────

.PHONY: build

build: ## Build server and ingest binaries with otel feature (release)
	cargo build --manifest-path $(ROOT_DIR)/Cargo.toml -p fluree-db-server --features otel --release
	cargo build --manifest-path $(ROOT_DIR)/Cargo.toml -p fluree-db-ingest --features otel --release
	@echo "Built: $(SERVER_BIN)"
	@echo "Built: $(INGEST_BIN)"

# ── Server lifecycle ─────────────────────────────────────────────────────────

.PHONY: server server-stop server-logs

server: ## Start fluree-server in background with OTEL export
	@if [ ! -x $(SERVER_BIN) ]; then echo "Binary not found: $(SERVER_BIN)"; echo "Run 'make build' first."; exit 1; fi
	@mkdir -p $(STORAGE) $(DATA_DIR)
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Server already running (PID $$(cat $(PID_FILE)))"; \
		exit 0; \
	fi
	@OTEL_SERVICE_NAME=fluree-server \
	OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
	RUST_LOG=$(RUST_LOG) \
	$(SERVER_BIN) \
		--storage-path $(STORAGE) \
		--listen-addr 0.0.0.0:$(PORT) \
		$(if $(filter true,$(INDEXING)),--indexing-enabled) \
		> $(LOG_FILE) 2>&1 & \
	echo $$! > $(PID_FILE) && \
	echo "Server starting (PID $$(cat $(PID_FILE))), logs: $(LOG_FILE)" && \
	./scripts/wait-for-server.sh $(BASE_URL) 30

server-stop: ## Stop the background server
	@if [ -f $(PID_FILE) ]; then \
		PID=$$(cat $(PID_FILE)); \
		if kill -0 $$PID 2>/dev/null; then \
			kill $$PID; \
			echo "Server stopped (PID $$PID)"; \
		else \
			echo "Server not running (stale PID $$PID)"; \
		fi; \
		rm -f $(PID_FILE); \
	else \
		echo "No PID file found"; \
	fi

server-logs: ## Tail server stdout/stderr
	@if [ -f $(LOG_FILE) ]; then \
		tail -f $(LOG_FILE); \
	else \
		echo "No log file found at $(LOG_FILE)"; \
	fi

# ── Scenarios ────────────────────────────────────────────────────────────────

.PHONY: transact query index firehose smoke cycle stress

transact: ## Minimal transaction scenarios (insert, upsert, update, turtle, SPARQL UPDATE)
	./scripts/transact-smoke.sh $(BASE_URL) $(LEDGER)

query: ## Seed data + query via FQL and SPARQL
	./scripts/query-smoke.sh $(BASE_URL) $(LEDGER)

index: ## Insert enough data to trigger indexing, then query indexed data
	./scripts/index-smoke.sh $(BASE_URL) $(LEDGER)

firehose: generate ## Generate large dataset, bulk import via fluree-ingest
	@if [ ! -x $(INGEST_BIN) ]; then echo "Binary not found: $(INGEST_BIN)"; echo "Run 'make build' first."; exit 1; fi
	./scripts/firehose.sh $(INGEST_BIN) $(GEN_DIR) $(DATA_DIR)/storage-firehose $(ENTITIES) $(PARSE_THREADS)

smoke: ## Full cycle: seed, transact, query, index — end-to-end span waterfall
	./scripts/seed-ledger.sh $(BASE_URL) $(LEDGER)
	./scripts/full-cycle.sh $(BASE_URL) $(LEDGER)

stress: ## High-volume stress test: 50K inserts with backpressure + expensive queries
	./scripts/stress-test.sh $(BASE_URL) $(LEDGER) $(STRESS_PRODUCTS) $(STRESS_BATCH)

cycle: ## Sustained transact + query workload triggering multiple index cycles
	@echo "Running 3 full cycles to trigger multiple index rebuilds..."
	@for i in 1 2 3; do \
		echo ""; \
		echo "=== Cycle $$i/3 ==="; \
		./scripts/full-cycle.sh $(BASE_URL) $(LEDGER); \
		sleep 2; \
	done
	@echo ""
	@echo "Done. Check Jaeger for multi-cycle trace patterns: http://localhost:16686"

# ── Data generation & cleanup ────────────────────────────────────────────────

.PHONY: generate clean clean-all nuke

generate: ## Generate TTL data files (configurable: ENTITIES, PROPS)
	./scripts/generate-data.sh $(ENTITIES) $(PROPS) $(GEN_DIR)

clean: server-stop ## Remove _data/ (generated storage, TTL, PIDs)
	rm -rf $(DATA_DIR)
	@echo "Cleaned: $(DATA_DIR)"

clean-all: clean down ## clean + stop Docker

nuke: clean-all ## clean-all + remove compiled binaries
	rm -f $(SERVER_BIN) $(INGEST_BIN)
	@echo "Removed compiled binaries"

# ── Convenience combos ───────────────────────────────────────────────────────

.PHONY: all fresh

all: up build server smoke ## Full setup: Jaeger + build + server + smoke test
	@echo ""
	@echo "All done. Open Jaeger: http://localhost:16686"
	@echo "Search for service: fluree-server"

fresh: reset clean build server smoke ## Clean-slate re-run: reset Jaeger + rebuild + smoke
	@echo ""
	@echo "Fresh run complete. Open Jaeger: http://localhost:16686"

# ── Help ─────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@echo "OTEL Testing Infrastructure"
	@echo ""
	@echo "Usage: make [target] [VAR=value ...]"
	@echo ""
	@echo "Configurable variables:"
	@echo "  PORT=$(PORT)            Server listen port"
	@echo "  LEDGER=$(LEDGER)    Ledger alias"
	@echo "  ENTITIES=$(ENTITIES)       Entity count for data generation"
	@echo "  INDEXING=$(INDEXING)         Enable background indexing"
	@echo "  PARSE_THREADS=$(PARSE_THREADS)      Parallel parse threads for firehose"
	@echo "  RUST_LOG=...            Log filter (see README.md)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'
