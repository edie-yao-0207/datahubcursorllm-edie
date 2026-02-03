from dataclasses import dataclass

from dataweb.userpkgs.firmware.table import DatabaseTable


@dataclass
class AnyUpstream(str):
    table: DatabaseTable

    def __str__(self):
        return f"{self.table}"
