#!/usr/bin/env python3
"""
Databricks AI/BI Dashboard Generator (Config-Driven)

Generates a .lvdash.json file from a JSON configuration file.
The config defines tabs, widgets, and visualization settings.
SQL files provide the queries and are located relative to the config file.

Usage:
    python generate_from_config.py <path/to/config.json> > dashboard.lvdash.json
    python generate_from_config.py dashboards/dce/dashboard_config.json > dce_dashboard.lvdash.json

The generator looks for SQL files relative to the config file's directory.
This allows the same generator to be used for multiple dashboards.
"""

import json
import sys
from pathlib import Path

# Add parent directories to path so we can import from the package
# This allows the script to be run directly: python scripts/generate_from_config.py
script_dir = Path(__file__).resolve().parent
dashboards_dir = script_dir.parent
backend_dir = dashboards_dir.parent.parent
sys.path.insert(0, str(backend_dir))

from dataplatform.dashboards.core import ConfigDrivenGenerator


def main():
    """Generate dashboard from config."""
    # Require config path argument
    if len(sys.argv) < 2:
        print("Usage: python generate_from_config.py <config.json>", file=sys.stderr)
        print("", file=sys.stderr)
        print("Example:", file=sys.stderr)
        print("  python generate_from_config.py dashboards/dce/dashboard_config.json > dce_dashboard.lvdash.json", file=sys.stderr)
        sys.exit(1)

    config_path = Path(sys.argv[1])

    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    # Generate dashboard
    generator = ConfigDrivenGenerator(config_path)
    dashboard = generator.generate()

    # Output JSON (must be the only thing on stdout for proper redirection)
    print(json.dumps(dashboard, indent=2))


if __name__ == "__main__":
    main()

