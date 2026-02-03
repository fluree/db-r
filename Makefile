.PHONY: bench bench-quick bench-large bench-concurrent bench-full bench-queries \
       bench-otel bench-otel-setup bench-otel-teardown

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

# Default: 10 MB ingest, sequential, in-memory
bench:
	cargo run --release -p fluree-bench -- ingest --data-size-mb 10

# Quick smoke test (1 MB)
bench-quick:
	cargo run --release -p fluree-bench -- ingest --data-size-mb 1

# Large dataset with concurrency
bench-large:
	cargo run --release -p fluree-bench -- ingest --data-size-mb 100 --concurrency 4

# Concurrency stress test
bench-concurrent:
	cargo run --release -p fluree-bench -- ingest --data-size-mb 10 --concurrency 8

# Full: ingest + query matrix (in-memory)
bench-full:
	cargo run --release -p fluree-bench -- full --data-size-mb 10

# Query matrix only (needs prior ingest with --storage)
bench-queries:
	cargo run --release -p fluree-bench -- query --data-size-mb 10

# With OTEL export: auto-starts Jaeger, runs benchmark, prints UI URL
bench-otel:
	./benchmarks/fluree-bench/scripts/bench-otel.sh ingest --data-size-mb 10 --concurrency 4

# Jaeger lifecycle (for manual use)
bench-otel-setup:
	docker run -d --name fluree-bench-jaeger -p 4317:4317 -p 16686:16686 jaegertracing/jaeger:latest

bench-otel-teardown:
	docker rm -f fluree-bench-jaeger
