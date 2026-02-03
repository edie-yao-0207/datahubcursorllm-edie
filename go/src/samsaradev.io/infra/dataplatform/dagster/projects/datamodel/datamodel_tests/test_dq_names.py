import warnings

import pytest
from dagster import AssetsDefinition
from datamodel import defs


def test_dq_names():
    dq_check_names = []
    repo_def = defs.get_repository_def()
    for asset_key, _ in repo_def.assets_defs_by_key.items():
        asset_def: AssetsDefinition = repo_def.assets_defs_by_key[asset_key]
        asset_spec = asset_def.get_asset_spec()
        region, database, table = tuple(asset_spec.key.path)
        if table.startswith("dq_"):
            # only want to run this check against one version of the asset
            if (
                asset_spec.metadata.get("dq_checks") is not None
                and region == "us-west-2"
            ):
                dq_checks = asset_spec.metadata.get("dq_checks", [])
                for dq_check in dq_checks:
                    assert (
                        dq_check.get("name") not in dq_check_names
                    ), f"DQ check {dq_check.get('name')} is not unique across all assets"
                    dq_check_names.append(dq_check.get("name"))
