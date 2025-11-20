# DevOnly Common Utils Makefile
# Inherits common targets from ../Makefile.common

PROJECT_NAME := clickhouse-sink

# Include common Makefile
include ../Makefile.common

# Crate-specific targets
.PHONY: publish publish-docs

# Generate documentation
docs:
	@echo "Generating documentation for $(PROJECT_NAME)..."
	cargo doc --no-deps --open

# Override common targets to add sync-lockfile dependency
build: sync-lockfile
	@echo "Building $(PROJECT_NAME)..."
	cargo build

build-release: sync-lockfile
	@echo "Building $(PROJECT_NAME) in release mode..."
	cargo build --release

test: sync-lockfile
	@echo "Running tests..."
	cargo test

check: sync-lockfile
	@echo "Checking code..."
	cargo check

dev-setup: sync-lockfile install-deps fmt lint check test-unit
	@echo "Running examples (if available)..."
	@$(MAKE) -s examples 2>/dev/null || echo "No examples to run"
	@echo "Development setup complete for $(PROJECT_NAME)!"

pre-commit: sync-lockfile fmt lint-strict test examples
	@echo "Pre-commit checks passed for $(PROJECT_NAME)!"

all: sync-lockfile fmt lint test build examples
	@echo "All checks passed for $(PROJECT_NAME)!"

ci: sync-lockfile check test build-release audit examples
	@echo "CI pipeline completed successfully for $(PROJECT_NAME)!"
