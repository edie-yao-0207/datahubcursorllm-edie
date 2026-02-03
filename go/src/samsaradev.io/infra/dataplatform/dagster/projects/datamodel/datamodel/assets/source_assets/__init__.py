from types import ModuleType
from typing import List

from dagster import SourceAsset
from dagster._core.definitions.load_assets_from_modules import find_modules_in_package


def load_source_assets(module: ModuleType) -> List[SourceAsset]:
    source_assets = []
    modules = find_modules_in_package(module)
    for module in modules:
        for attr in dir(module):
            value = getattr(module, attr)
            if isinstance(value, dict):
                for _, item in value.items():
                    if isinstance(item, SourceAsset):
                        source_assets.append(item)
    return source_assets
