from dataclasses import dataclass
from typing import Optional


@dataclass
class SnowflakeQuery:
    """
    Simple data class representing a query that needs to be executed in Snowflake.
    """

    operation_id: str
    query: str
    timeout: Optional[int]
