import logging
import socket
from typing import List

import requests
from requests import HTTPError

from agent.utils.utils import LOCAL

logger = logging.getLogger(__name__)


class MetricsService:
    @staticmethod
    def fetch_metrics() -> List[str]:
        if LOCAL:
            return LocalMetricsService.fetch_metrics()
        return SnowparkMetricsService.fetch_metrics()


class SnowparkMetricsService:
    @staticmethod
    def fetch_metrics() -> List[str]:
        """
        Fetches metrics using Snowpark Monitoring Services:
        https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#accessing-compute-pool-metrics
        The way it works is that you need to resolve all IP addresses mapped to the discover host
        name: discover.monitor.<COMPUTE_POOL_NAME>.snowflakecomputing.internal, and then
        request metrics for each IP address from http://<IP_ADDRESS>:9001/metrics.
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
            try:
                response = requests.get(f"http://{address}:9001/metrics")
                response.raise_for_status()
                lines.extend(response.text.splitlines())
            except HTTPError as exc:
                logger.error(f"Failed to fetch metrics from {address}: {exc}")
        return lines


class LocalMetricsService:
    @staticmethod
    def fetch_metrics() -> List[str]:
        # used only for testing
        return ['metric_1{host="abc.com",resource="cpu"} 1', "metric_2 2"]
