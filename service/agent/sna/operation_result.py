from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class AgentOperationResult:
    """
    Data class to represent the result of an operation
    """

    operation_id: str
    result: Optional[Dict[str, Any]] = None
    query_id: Optional[str] = None
