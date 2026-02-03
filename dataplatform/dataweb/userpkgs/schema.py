from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Union


@dataclass
class Column:
    name: str
    type: Union[str, Dict[str, Any]]
    comment: str
    nullable: Optional[bool] = field(default=False)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "nullable": self.nullable,
            "metadata": {
                "comment": self.comment,
            },
        }
