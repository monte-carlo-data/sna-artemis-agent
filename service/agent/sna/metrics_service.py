from typing import List

import requests


class MetricsService:
    @staticmethod
    def fetch_metrics() -> List[str]:
        """
        Fetches metrics using Snowpark Monitoring Services:
        https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#accessing-compute-pool-metrics
        """
        response = requests.get(
            "http://discover.monitor.mcd_agent_compute_pool.snowflakecomputing.internal:9001/metrics"
        )
        lines = response.text.splitlines()
        return lines
