from dataclasses import dataclass
from typing import Literal, TypeAlias


@dataclass
class Percentile:
    value: int

SLA: TypeAlias = Literal["avg"] | Percentile