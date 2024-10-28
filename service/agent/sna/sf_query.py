from dataclasses import dataclass
from typing import Optional


@dataclass
class SnowflakeQuery:
    operation_id: str
    query: str
    timeout: Optional[int]
