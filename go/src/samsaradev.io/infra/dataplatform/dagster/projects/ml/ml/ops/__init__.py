"""Ops module - automatically discovers and imports all ops.

This module dynamically imports all ops from Python files in this directory.
When you add a new op file (e.g., new_feature.py with new_feature_op),
it will be automatically discovered and imported - no manual updates needed!
"""

import importlib
import inspect
from pathlib import Path


def _auto_import_ops():
    """Automatically discover and import all ops from this directory.

    This function:
    1. Scans the ops directory for Python files
    2. Imports each module dynamically
    3. Extracts ops (*_op) and Config classes
    4. Returns them for module-level exposure
    """
    ops_dir = Path(__file__).parent
    discovered = {}

    # Find all Python files in this directory (excluding __init__.py)
    for py_file in ops_dir.glob("*.py"):
        if py_file.name == "__init__.py":
            continue

        # Import the module dynamically
        module_name = f"ml.ops.{py_file.stem}"
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue

        # Extract all ops and Config classes from the module
        for name, obj in inspect.getmembers(module):
            # Include ops (callable things ending in _op)
            if name.endswith("_op") and callable(obj):
                discovered[name] = obj
            # Include Config classes
            elif name.endswith("Config") and inspect.isclass(obj):
                discovered[name] = obj

    return discovered


# Auto-discover and import all ops
_discovered_items = _auto_import_ops()
globals().update(_discovered_items)

# Build __all__ dynamically for linting and documentation
__all__ = sorted(_discovered_items.keys())
