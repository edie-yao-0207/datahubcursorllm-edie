import warnings

import pytest
from dagster import AssetsDefinition
from datamodel import defs
from datamodel.common.constants import GlossaryTerm


def test_glossary_terms():
    repo_def = defs.get_repository_def()
    for asset_key, _ in repo_def.assets_defs_by_key.items():
        asset_def: AssetsDefinition = repo_def.assets_defs_by_key[asset_key]
        asset_spec = asset_def.get_asset_spec()
        region, database, table = tuple(asset_spec.key.path)
        if (
            asset_spec.metadata.get("glossary_terms") is not None
            and region == "us-west-2"
        ):
            glossary_terms = asset_spec.metadata.get("glossary_terms", [])
            for glossary_term in glossary_terms:
                assert glossary_term in [
                    g.value for g in GlossaryTerm
                ], f"Asset Key {asset_key} has an invalid glossary term {glossary_term}"
