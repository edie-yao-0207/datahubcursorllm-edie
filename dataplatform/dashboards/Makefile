# Databricks AI/BI Dashboard Generation
#
# Convention-based dashboard generator. Each dashboard is a directory containing:
#   - dashboard_config.json  (required for config mode)
#   - queries/               (SQL files)
#
# Usage:
#   make all              - Rebuild all dashboards
#   make <dashboard>      - Rebuild specific dashboard (e.g., make dce)
#   make clean            - Remove generated dashboards
#   make validate         - Validate all generated JSON files
#   make list             - List all dashboards and their SQL files
#
# Adding a new dashboard:
#   1. Create directory: mkdir dashboards/myapp
#   2. Add config: dashboards/myapp/dashboard_config.json
#   3. Add queries: dashboards/myapp/queries/*.sql
#   4. Build: make myapp

PYTHON := python3

# Generator scripts
GEN_AUTO := scripts/generate_dashboard.py
GEN_CONFIG := scripts/generate_from_config.py

# Output directory for generated dashboards
OUTPUT_DIR := output
# Directory for commitable text versions of dashboards
COMMITABLE_DIR := configs

# Auto-discover dashboards (directories containing dashboard_config.json in dashboards/)
DASHBOARDS := $(patsubst dashboards/%/dashboard_config.json,%,$(wildcard dashboards/*/dashboard_config.json))

.PHONY: all clean validate list help clean-orphaned $(DASHBOARDS)

# =============================================================================
# Default target
# =============================================================================

all: $(DASHBOARDS)

help:
	@echo "Databricks Dashboard Generator"
	@echo ""
	@echo "Available dashboards:"
	@for d in $(DASHBOARDS); do echo "  - $$d"; done
	@echo ""
	@echo "Usage:"
	@echo "  make all              - Rebuild all dashboards"
	@echo "  make <dashboard>      - Rebuild specific dashboard"
	@echo "  make clean            - Remove generated dashboards"
	@echo "  make validate         - Validate all generated JSON files"
	@echo "  make list             - List dashboards and SQL files"
	@echo ""
	@echo "Adding a new dashboard:"
	@echo "  1. mkdir dashboards/<name>"
	@echo "  2. Create dashboards/<name>/dashboard_config.json"
	@echo "  3. Add SQL files to dashboards/<name>/queries/"
	@echo "  4. Run: make <name>"

# =============================================================================
# Generic dashboard build (config-driven mode)
# =============================================================================

# Pattern rule: make <dashboard> builds from dashboards/<dashboard>/dashboard_config.json
$(DASHBOARDS): %: dashboards/%/dashboard_config.json $(GEN_CONFIG)
	@echo "Building dashboard: $@"
	@mkdir -p "$(OUTPUT_DIR)" "$(COMMITABLE_DIR)"
	@dashboard_name=$$($(PYTHON) -c "import json; print(json.load(open('dashboards/$@/dashboard_config.json'))['dashboard']['name'])" 2>/dev/null || echo '$@'); \
	output_file="$(OUTPUT_DIR)/$$dashboard_name.lvdash.json"; \
	sanitized_name=$$(echo "$$dashboard_name" | tr ' ' '_'); \
	commitable_file="$(COMMITABLE_DIR)/$$sanitized_name.lvdash.txt"; \
	if $(PYTHON) $(GEN_CONFIG) "dashboards/$@/dashboard_config.json" > "$$output_file"; then \
		file_size=$$(stat -f%z "$$output_file" 2>/dev/null || stat -c%s "$$output_file" 2>/dev/null || echo 0); \
		if [ "$$file_size" -eq 0 ]; then \
			echo "✗ Build failed for $@: output file is empty"; \
			rm -f "$$output_file"; \
			exit 1; \
		fi; \
		cp "$$output_file" "$$commitable_file" || { echo "✗ Build failed for $@: failed to copy to configs/"; exit 1; }; \
		$(PYTHON) -c "import json; d=json.load(open('$$output_file')); print('✓ Generated $$output_file'); print('✓ Copied to $$commitable_file (commitable)'); print(f\"  - {len(d['datasets'])} datasets\"); print(f\"  - {len(d['pages'])} tabs\")"; \
		orphaned_file="$(OUTPUT_DIR)/$@.lvdash.json"; \
		if [ -f "$$orphaned_file" ] && [ "$$orphaned_file" != "$$output_file" ]; then \
			rm -f "$$orphaned_file"; \
		fi; \
	else \
		echo "✗ Build failed for $@"; \
		exit 1; \
	fi

# =============================================================================
# Utility targets
# =============================================================================

clean:
	@echo "Removing generated dashboards..."
	@rm -rf "$(OUTPUT_DIR)"
	@find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "✓ Cleaned (note: $(COMMITABLE_DIR)/ files are preserved)"

# Clean up orphaned/empty dashboard files (files that don't match expected dashboard names)
clean-orphaned:
	@if [ -d "$(OUTPUT_DIR)" ]; then \
		for f in "$(OUTPUT_DIR)"/*.lvdash.json; do \
			if [ -f "$$f" ]; then \
				file_size=$$(stat -f%z "$$f" 2>/dev/null || stat -c%s "$$f" 2>/dev/null || echo 0); \
				if [ "$$file_size" -eq 0 ]; then \
					echo "  Removing empty file: $$f"; \
					rm -f "$$f"; \
				fi; \
			fi; \
		done; \
		if [ -n "$(DASHBOARD_DIR)" ]; then \
			orphaned_file="$(OUTPUT_DIR)/$(DASHBOARD_DIR).lvdash.json"; \
			if [ -f "$$orphaned_file" ]; then \
				dashboard_name=$$($(PYTHON) -c "import json; print(json.load(open('dashboards/$(DASHBOARD_DIR)/dashboard_config.json'))['dashboard']['name'])" 2>/dev/null || echo ''); \
				if [ -n "$$dashboard_name" ] && [ "$$dashboard_name" != "$(DASHBOARD_DIR)" ]; then \
					expected_file="$(OUTPUT_DIR)/$$dashboard_name.lvdash.json"; \
					if [ -f "$$expected_file" ] && [ "$$orphaned_file" != "$$expected_file" ]; then \
						echo "  Removing orphaned file: $$orphaned_file (expected: $$expected_file)"; \
						rm -f "$$orphaned_file"; \
					fi; \
				fi; \
			fi; \
		fi; \
	fi

validate:
	@echo "Validating generated dashboards..."
	@for f in $(OUTPUT_DIR)/*.lvdash.json; do \
		if [ -f "$$f" ]; then \
			echo "Validating $$f..."; \
			$(PYTHON) -c "import json; json.load(open('$$f')); print('  ✓ Valid JSON')"; \
			$(PYTHON) -c "import json; d=json.load(open('$$f')); print(f'    Datasets: {len(d[\"datasets\"])}'); print(f'    Pages: {len(d[\"pages\"])}'); print(f'    Widgets: {sum(len(p[\"layout\"]) for p in d[\"pages\"])}')"; \
		fi; \
	done

list:
	@echo "Discovered dashboards:"
	@echo ""
	@for d in $(DASHBOARDS); do \
		echo "━━━ $$d ━━━"; \
		if [ -f "dashboards/$$d/dashboard_config.json" ]; then \
			name=$$($(PYTHON) -c "import json; print(json.load(open('dashboards/$$d/dashboard_config.json'))['dashboard']['name'])" 2>/dev/null); \
			echo "  Name: $$name"; \
		fi; \
		sql_count=$$(find "dashboards/$$d/queries" -name "*.sql" 2>/dev/null | wc -l | tr -d ' '); \
		echo "  SQL files: $$sql_count"; \
		echo ""; \
	done

# =============================================================================
# Development helpers
# =============================================================================

# Watch for changes and auto-rebuild (requires fswatch)
watch:
	@echo "Watching for changes in dashboard directories..."
	@fswatch -o $(addsuffix /queries/**/*.sql,$(addprefix dashboards/,$(DASHBOARDS))) $(addsuffix /dashboard_config.json,$(addprefix dashboards/,$(DASHBOARDS))) | \
		xargs -n1 -I{} make all
