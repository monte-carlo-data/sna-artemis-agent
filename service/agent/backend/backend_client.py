import json
import logging
from typing import Dict, Any, Optional
from urllib.parse import urljoin

import requests

from agent.utils.serde import AgentSerializer
from agent.utils.utils import BACKEND_SERVICE_URL, AGENT_ID, get_mc_login_token

logger = logging.getLogger(__name__)


class BackendClient:
    """
    Client used to interact with the MC Backend (Orchestrator) service.
    """

    @staticmethod
    def push_results(operation_id: str, result: Dict[str, Any]):
        """
        Pushes the result for a given operation, please note results are sent by a separate thread.
        See `ResultsPublisher` for more information.
        """
        logger.info(f"Sending query results to backend")
        try:
            results_url = urljoin(
                BACKEND_SERVICE_URL, f"/api/v1/agent/operations/{operation_id}/result"
            )
            result_str = json.dumps(
                {
                    "agent_id": AGENT_ID,
                    "result": result,
                },
                cls=AgentSerializer,
            )
            logger.info(f"Sending result to backend: {result_str[:500]}")
            response = requests.post(
                results_url,
                data=result_str,
                headers={
                    "Content-Type": "application/json",
                    **get_mc_login_token(),
                },
            )
            logger.info(
                f"Sent query results to backend, response: {response.status_code}"
            )
        except Exception as ex:
            logger.error(f"Failed to push results to backend: {ex}")

    @staticmethod
    def execute_operation(
        path: str, method: str = "GET", body: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Performs an operation on the backend service. For example `ping`.
        """
        try:
            url = urljoin(BACKEND_SERVICE_URL, path)
            headers = get_mc_login_token()
            if body:
                headers["Content-Type"] = "application/json"
            response = requests.request(
                method=method,
                url=url,
                json=body,
                headers=headers,
            )
            logger.info(
                f"Sent backend request {path}, response: {response.status_code}"
            )
            response.raise_for_status()
            return response.json() or {"error": "empty response"}
        except Exception as ex:
            logger.error(f"Error sending request to backend: {ex}")
            return {
                "error": str(ex),
            }

    @classmethod
    def download_operation(cls, operation_id: str) -> Dict:
        """
        Download the full body for an operation, `SSE` has a limit in the size of the events, so
        when the operation exceeds that size we perform an additional request to get the full
        operation.
        """
        operation = cls.execute_operation(
            f"/api/v1/agent/operations/{operation_id}/request"
        )
        if error_message := operation.get("error"):
            raise Exception(
                f"Failed to download operation {operation_id}: {error_message}"
            )
        return operation
