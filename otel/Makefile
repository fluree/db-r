# OTEL Deep Tracing — Test Harness
#
# Usage:
#   make all     — Full setup: Jaeger + build + server + smoke tests
#   make fresh   — Clean slate: stop everything, then full setup
#   make smoke   — Seed ledger + run queries + transactions
#   make stress  — Sustained load (batch inserts + expensive queries)
#   make ui      — Open Jaeger in browser
#   make down    — Stop everything

.PHONY: all fresh up down build server server-stop smoke stress ui seed query transact clean

# ── Configuration ──────────────────────────────────────────────
PORT          ?= 8090
LEDGER        ?= otel-test:main
ENTITIES      ?= 100000
RUST_LOG      ?= info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug

ROOT_DIR      := $(shell cd .. && pwd)
SERVER_BIN    := $(ROOT_DIR)/target/debug/fluree-server
STORAGE       := $(ROOT_DIR)/target/otel-data
LOG_FILE      := server.log
PID_FILE      := server.pid

OTEL_SERVICE_NAME             := fluree-server
OTEL_EXPORTER_OTLP_ENDPOINT   := http://localhost:4317
OTEL_EXPORTER_OTLP_PROTOCOL   := grpc

export PORT LEDGER ENTITIES

# ── Composite targets ─────────────────────────────────────────
all: up build server smoke
	@echo "\n=== All done. Run 'make ui' to inspect traces in Jaeger ==="

fresh: server-stop down clean all

# ── Infrastructure ─────────────────────────────────────────────
up:
	@echo "=== Starting Jaeger ==="
	docker compose up -d
	@echo "Jaeger UI: http://localhost:16686"

down:
	@echo "=== Stopping Jaeger ==="
	docker compose down

# ── Build ──────────────────────────────────────────────────────
build:
	@echo "=== Building fluree-server with otel feature ==="
	cd $(ROOT_DIR) && cargo build -p fluree-db-server --features otel

# ── Server lifecycle ───────────────────────────────────────────
server: server-stop
	@mkdir -p $(STORAGE)
	@echo "=== Starting fluree-server (port=$(PORT), log=$(LOG_FILE)) ==="
	OTEL_SERVICE_NAME=$(OTEL_SERVICE_NAME) \
	OTEL_EXPORTER_OTLP_ENDPOINT=$(OTEL_EXPORTER_OTLP_ENDPOINT) \
	OTEL_EXPORTER_OTLP_PROTOCOL=$(OTEL_EXPORTER_OTLP_PROTOCOL) \
	RUST_LOG=$(RUST_LOG) \
	$(SERVER_BIN) \
		--storage-path $(STORAGE) \
		--listen-addr 0.0.0.0:$(PORT) \
		> $(LOG_FILE) 2>&1 & echo $$! > $(PID_FILE)
	@./scripts/wait-for-server.sh $(PORT)

server-stop:
	@if [ -f $(PID_FILE) ]; then \
		PID=$$(cat $(PID_FILE)); \
		if kill -0 $$PID 2>/dev/null; then \
			echo "=== Stopping fluree-server (pid=$$PID) ==="; \
			kill $$PID && sleep 1; \
			kill -0 $$PID 2>/dev/null && kill -9 $$PID; \
		fi; \
		rm -f $(PID_FILE); \
	fi

# ── Test scenarios ─────────────────────────────────────────────
smoke: seed query transact
	@echo "\n=== Smoke tests complete. Check Jaeger: http://localhost:16686 ==="

seed:
	@echo "=== Seeding ledger ==="
	./scripts/seed-ledger.sh

query:
	@echo "=== Running query smoke tests ==="
	./scripts/query-smoke.sh

transact:
	@echo "=== Running transact smoke tests ==="
	./scripts/transact-smoke.sh

stress:
	@echo "=== Running stress test ==="
	./scripts/stress-test.sh

# ── Utilities ──────────────────────────────────────────────────
ui:
	@echo "Opening Jaeger UI..."
	@open http://localhost:16686 2>/dev/null || xdg-open http://localhost:16686 2>/dev/null || echo "Visit http://localhost:16686"

clean:
	@echo "=== Cleaning storage + logs ==="
	rm -rf $(STORAGE) $(LOG_FILE) $(PID_FILE)

logs:
	@tail -f $(LOG_FILE)
