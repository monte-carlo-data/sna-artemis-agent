from dataclasses import dataclass
from typing import Optional

from apollo.egress.agent.service.operation_result import OperationAttributes


@dataclass
class SnowflakeQuery:
    """
    Simple data class representing a query that needs to be executed in Snowflake.
    """

    operation_id: str
    query: str
    timeout: Optional[int]
    operation_attrs: OperationAttributes
