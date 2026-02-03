"""Tests to ensure ops module exports are properly maintained."""

import inspect


def test_all_ops_in_dunder_all():
    """Test that all ops defined in ml.ops are included in __all__.

    This ensures that when new ops are added, they're properly exported
    from the ml.ops module for linting and documentation purposes.
    """
    from ml import ops

    # Get all attributes in the ops module
    all_attrs = dir(ops)

    # Find all ops (things ending in _op that are callable)
    actual_ops = [
        name
        for name in all_attrs
        if name.endswith("_op") and callable(getattr(ops, name))
    ]

    # Get what's declared in __all__
    declared_exports = ops.__all__ if hasattr(ops, "__all__") else []

    # Find ops that are missing from __all__
    missing_from_all = [op for op in actual_ops if op not in declared_exports]

    assert len(missing_from_all) == 0, (
        f"The following ops are defined but not in ml.ops.__all__: {missing_from_all}. "
        "Please add them to the __all__ list in ml/ops/__init__.py"
    )


def test_all_op_configs_in_dunder_all():
    """Test that all Config classes for ops are included in __all__.

    Config classes should also be exported for type hints and documentation.
    """
    from ml import ops

    # Get all attributes in the ops module
    all_attrs = dir(ops)

    # Find all Config classes (names ending in Config that are classes)
    config_classes = []
    for name in all_attrs:
        if name.endswith("Config"):
            attr = getattr(ops, name)
            if inspect.isclass(attr):
                config_classes.append(name)

    # Get what's declared in __all__
    declared_exports = ops.__all__ if hasattr(ops, "__all__") else []

    # Find configs that are missing from __all__
    missing_configs = [cfg for cfg in config_classes if cfg not in declared_exports]

    assert len(missing_configs) == 0, (
        f"The following Config classes are defined but not in ml.ops.__all__: {missing_configs}. "
        "Please add them to the __all__ list in ml/ops/__init__.py"
    )


def test_ops_are_discoverable_by_loader():
    """Test that all ops in __all__ can be discovered by the auto-discovery mechanism.

    The loader looks for ops by pattern (*_op), so this validates the naming convention.
    """
    from ml import ops

    if not hasattr(ops, "__all__"):
        return  # Skip if no __all__ defined

    for name in ops.__all__:
        if not name.endswith("Config"):  # Skip config classes
            assert name.endswith("_op"), (
                f"Op '{name}' in __all__ doesn't follow naming convention (*_op). "
                "The auto-discovery mechanism requires ops to end with '_op'."
            )


def test_auto_discovery_finds_all_ops():
    """Test that the auto-discovery mechanism finds all defined ops.

    This is an integration test that validates the loader can discover
    all ops that are exported from ml.ops.
    """
    from ml.common.loaders import auto_discover_ops, load_job_configs_by_name

    # Load job configs to pass to auto_discover_ops
    job_configs = load_job_configs_by_name()

    # Run auto-discovery
    op_registry = auto_discover_ops(job_configs)

    # Should find at least the anomaly_detection and hello_world ops
    # (if they have corresponding job configs)
    assert len(op_registry) > 0, "Auto-discovery should find at least one op"

    # All discovered ops should be callable
    for job_name, op_or_ops in op_registry.items():
        if isinstance(op_or_ops, list):
            for op in op_or_ops:
                assert callable(op), f"Op in job '{job_name}' is not callable"
        else:
            assert callable(op_or_ops), f"Op for job '{job_name}' is not callable"


def test_auto_discovery_fails_on_missing_ops():
    """Test that auto-discovery fails when any op in the ops array is missing.

    This is a critical safety check: if a job specifies ["op1", "op2", "op3"]
    but op2 doesn't exist, we must fail rather than silently creating a partial
    chain that skips op2's processing step. This prevents data corruption.
    """
    from ml.common.loaders import auto_discover_ops

    # Create a fake job config with a non-existent op in the middle
    fake_job_configs = {
        "test_job_with_missing_op": {
            "name": "test_job_with_missing_op",
            "owner": "DataEngineering",
            "regions": ["us-west-2"],
            "ops": ["hello_world", "nonexistent_op", "hello_world2"],
        }
    }

    # Should raise ValueError with clear message about missing op
    try:
        auto_discover_ops(fake_job_configs)
        pytest.fail(
            "auto_discover_ops should have raised ValueError for missing op "
            "but completed successfully instead"
        )
    except ValueError as e:
        error_msg = str(e)
        # Verify error message contains key information
        assert (
            "test_job_with_missing_op" in error_msg
        ), "Error message should mention the job name"
        assert (
            "nonexistent_op" in error_msg
        ), "Error message should mention the missing op"
        assert (
            "partial" in error_msg.lower()
        ), "Error message should warn about partial chains"
