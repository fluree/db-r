# testsuite-sparql/Makefile
#
# Developer targets for the W3C SPARQL compliance test suite.
# Run `make help` to see available targets.

CARGO := cargo
CRATE := testsuite-sparql
REPORT := report.txt
FAILURES_REPORT := failures.txt
RDF_TESTS := rdf-tests

# W3C URL prefix → local path prefix
W3C_PREFIX := https://w3c.github.io/rdf-tests/
LOCAL_PREFIX := $(RDF_TESTS)/

.PHONY: help test test-syntax11 test-syntax10 test-eval report failures \
        failures-list count investigate show-query check clean update-submodule

help: ## Show this help
	@echo "W3C SPARQL Compliance Test Suite"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make test                     Run all W3C tests"
	@echo "  make failures                 Show only failing tests"
	@echo "  make investigate TEST=...     Show context for a specific test"
	@echo "  make show-query TEST=...      Print the .rq file for a test"

# ---------------------------------------------------------------------------
# Running tests
# ---------------------------------------------------------------------------

test: ## Run all W3C SPARQL tests
	$(CARGO) test -p $(CRATE) -- --nocapture 2>&1

test-syntax11: ## Run SPARQL 1.1 syntax tests only
	$(CARGO) test -p $(CRATE) sparql11_syntax_query_tests -- --nocapture 2>&1

test-syntax10: ## Run SPARQL 1.0 syntax tests only
	$(CARGO) test -p $(CRATE) sparql10_syntax_tests -- --nocapture 2>&1

test-eval: ## Run SPARQL 1.1 query evaluation tests (Phase 2)
	$(CARGO) test -p $(CRATE) sparql11_query_w3c_testsuite -- --nocapture --include-ignored 2>&1

# ---------------------------------------------------------------------------
# Reports and analysis
# ---------------------------------------------------------------------------

report: ## Run all tests and save full output to report.txt
	@echo "Running W3C SPARQL tests (output → $(REPORT))..."
	@$(CARGO) test -p $(CRATE) -- --nocapture > $(REPORT) 2>&1; true
	@grep -E "(=== Test Summary|Total:|Passed:|Ignored:|Failed:)" $(REPORT) || true
	@echo "Full report saved to $(REPORT)"

failures: report ## Show only failing tests (extracted from report)
	@echo ""
	@awk '/failing test\(s\):/{found=1; print ""; print; next} found && /^test .*\.\.\. FAILED/{found=0; next} found{print}' $(REPORT) \
		|| echo "(no failures)"

failures-list: report ## One-line-per-failure summary (test ID + error type)
	@echo ""
	@echo "=== Failure Summary ==="
	@awk '/failing test\(s\):/{found=1; next} found && /^test .*\.\.\. FAILED/{found=0; next} found && /^http/{print}' $(REPORT) \
		|| echo "(no failures)"

count: ## Show pass/fail summary counts
	@$(CARGO) test -p $(CRATE) -- --nocapture 2>&1 | grep -E "(Total|Passed|Ignored|Failed|=== Test Summary)" || true

# ---------------------------------------------------------------------------
# Investigating specific tests
# ---------------------------------------------------------------------------

investigate: ## Show all context for a specific test (TEST=<test_url_or_fragment>)
ifndef TEST
	@echo "Usage: make investigate TEST=<test_id>"
	@echo ""
	@echo "TEST can be:"
	@echo "  - Full URL: https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl#test_34"
	@echo "  - Fragment: test_34  (will search report.txt for matches)"
	@exit 1
endif
	@echo "=== Test: $(TEST) ==="
	@echo ""
	@echo "--- Searching report for test ---"
	@grep "$(TEST)" $(REPORT) 2>/dev/null || echo "(not found in report — run 'make report' first)"
	@echo ""
	@echo "--- Matching .rq files ---"
	@find $(RDF_TESTS) -name "*.rq" | xargs grep -l "$(TEST)" 2>/dev/null || \
		find $(RDF_TESTS) -name "*$(TEST)*" 2>/dev/null || \
		echo "(no matching query files found)"

show-query: ## Print the .rq query file for a test (TEST=<path_fragment>)
ifndef TEST
	@echo "Usage: make show-query TEST=<filename>"
	@echo "Example: make show-query TEST=syntax-select-expr-04.rq"
	@exit 1
endif
	@find $(RDF_TESTS) -name "*$(TEST)*" -exec echo "--- {} ---" \; -exec cat {} \; 2>/dev/null || \
		echo "No file matching '$(TEST)' found in $(RDF_TESTS)/"

# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------

update-submodule: ## Update the W3C rdf-tests submodule to latest
	cd $(RDF_TESTS) && git pull origin main
	@echo ""
	@echo "Submodule updated. Run 'make test' to check for regressions."

check: ## Run cargo check on the testsuite crate
	$(CARGO) check -p $(CRATE)

clean: ## Remove generated report files
	rm -f $(REPORT) $(FAILURES_REPORT)
