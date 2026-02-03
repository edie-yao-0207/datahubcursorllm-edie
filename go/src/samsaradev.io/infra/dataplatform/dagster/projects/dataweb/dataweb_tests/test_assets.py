from dataclasses import asdict

# just importing the module will fail if the DAG can't be built
from dataweb import defs
from dataweb.userpkgs.constants import AWSRegion

DELTALAKE_DATE_PARTITION_NAME = "date"


def test_assets_key():

    repo_def = defs.get_repository_def()
    for asset_key, _ in repo_def.assets_defs_by_key.items():
        assert (
            len(asset_key.path) == 3
        ), f"{asset_key} must have length of 3 in format (region, database, table)."

        region = asset_key.path[0]
        valid_regions = list(asdict(AWSRegion()).values())
        assert (
            region in valid_regions
        ), f"Invalid AWS region '{region}' for asset {asset_key}. Must be one of {valid_regions}."


def test_asset_deps():

    repo_def = defs.get_repository_def()
    for asset_key, asset_def in repo_def.assets_defs_by_key.items():
        for dep_asset_key in asset_def.asset_deps[asset_key]:
            assert (
                len(dep_asset_key.path) == 3
            ), f"Invalid asset key: '{asset_key}' must have length of 3 in the order (region, database, table)."

            region = dep_asset_key.path[0]
            valid_regions = list(asdict(AWSRegion()).values())
            assert (
                region in valid_regions
            ), f"Invalid AWS region '{region}' for dep {dep_asset_key}. Must be one of {valid_regions}."
