import logging
import socket
from typing import List

import requests

logger = logging.getLogger(__name__)


class MetricsService:
    @staticmethod
    def fetch_metrics() -> List[str]:
        """
        Fetches metrics using Snowpark Monitoring Services:
        https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#accessing-compute-pool-metrics
        """
        discover_host_name = (
            "discover.monitor.mcd_agent_compute_pool.snowflakecomputing.internal"
        )
        lookup_result = socket.getaddrinfo(discover_host_name, 80)
        addresses = set([addr[4][0] for addr in lookup_result])

        logger.info(f"{discover_host_name} resolves to: {addresses}")
        lines = []
        for address in addresses:
            logger.info(f"Requesting metrics from {address}")
            response = requests.get(f"http://{address}:9001/metrics")
            lines.extend(response.text.splitlines())
        return lines
