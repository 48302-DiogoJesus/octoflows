from dataclasses import dataclass
from typing import Literal, TypeAlias


@dataclass
class Percentile:
    value: int # higher => more confidence

SLA: TypeAlias = Literal["average"] | Percentile