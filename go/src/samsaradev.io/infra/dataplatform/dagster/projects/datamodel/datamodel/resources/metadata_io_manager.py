from typing import Dict, Tuple

from dagster import InputContext, OutputContext
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from dagster._core.storage.io_manager import IOManager


class MetadataIOManager(IOManager):
    """Inheriting from the Dagster I/O manager, this I/O manager will returns metadata defined in the asset stage.
    This metadata is then queryable downstream. For example, when leveraging asset sensors this metadata is accessible via the asset sensor context
    """

    def __init__(self):
        pass

    def handle_output(self, context: OutputContext, obj: object):
        return context.metadata

    def load_input(self, context: InputContext) -> None:
        return None
