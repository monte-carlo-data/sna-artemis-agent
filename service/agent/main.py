import json
import logging
import os
import signal
import sys
from typing import Any

from flask import Flask
from flask import make_response
from flask import request

from agent.sna.config.config_manager import ConfigurationManager
from agent.sna.config.db_config import DbConfig
from agent.sna.config.local_config import LocalConfig
from agent.utils.utils import enable_tcp_keep_alive, init_logging, LOCAL

init_logging()
logger = logging.getLogger(__name__)

from agent.sna.metrics_service import MetricsService
from agent.sna.sna_service import SnaService

SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVICE_PORT = os.getenv("SERVER_PORT") or "8081"
USE_DB_CONFIG_PERSISTENCE = (
    os.getenv("USE_DB_CONFIG_PERSISTENCE", "false" if LOCAL else "true").lower()
    == "true"
)


"""
This is the main entry point for the Agent service, it starts a Flask application
and the `SnaService` that will handle the communication with the MC backend.
It defines a few HTTP endpoints that will be used to communicate with the `Streamlit` app
and as UDF functions (as callbacks for query completion and failure).
"""

app = Flask(__name__)
service = SnaService(
    config_manager=ConfigurationManager(
        persistence=DbConfig() if USE_DB_CONFIG_PERSISTENCE else LocalConfig()
    )
)


def handler(signum: int, frame: Any):
    print("Signal handler called with signal", signum)
    service.stop()
    print("Signal handler completed")
    sys.exit(0)


signal.signal(signal.SIGINT, handler)


@app.get("/api/v1/test/healthcheck")
def health_check():
    """
    Used for readiness probe from the Snowflake platform.
    """
    return "OK"


@app.post("/api/v1/test/health")
def api_health():
    """
    Intended to be used from the Streamlit application, this gets called through a
    Snowflake function.
    """
    health_response = service.health_information()
    output_rows = [[0, json.dumps(health_response)]]
    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    return response


@app.get("/api/v1/test/health")
def health():
    """
    Intended to be used for local troubleshooting, not from the Streamlit application.
    """
    health_response = service.health_information(trace_id=request.args.get("trace_id"))
    response = make_response(health_response)
    response.headers["Content-type"] = "application/json"
    return response


@app.post("/api/v1/test/reachability")
def run_reachability_test():
    """
    Intended to be used from the Streamlit application, this gets called through a
    Snowflake function.
    """
    reachability_response = service.run_reachability_test()
    output_rows = [[0, json.dumps(reachability_response)]]
    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    return response


@app.post("/api/v1/agent/execute/snowflake/query_completed")
def query_completed():
    """
    Intended to be invoked from the stored procedure used when queries are executed asynchronously.
    It gets called through a Snowflake function.
    """
    message = request.json
    logger.debug(f"Received query completed: {message}")

    if message is None or not message["data"]:
        logger.info("Received empty message")
        return {}

    input_rows = message["data"]
    if input_rows:
        op_id = input_rows[0][1]
        query_id = input_rows[0][2]
        logger.info(f"QUERY COMPLETED: op_id={op_id}, query_id={query_id}")
        service.query_completed(op_id, query_id)

    output_rows = [[0, "ok"]]
    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    return response


@app.post("/api/v1/agent/execute/snowflake/query_failed")
def query_failed():
    """
    Intended to be invoked from the stored procedure used when queries are executed asynchronously.
    It gets called through a Snowflake function.
    """
    message = request.json
    logger.debug(f"Received query failed: {message}")

    if message is None or not message["data"]:
        logger.info("Received empty message")
        return {}

    input_rows = message["data"]
    if input_rows:
        operation_id = input_rows[0][1]
        code = input_rows[0][2]
        msg = input_rows[0][3]
        state = input_rows[0][4]
        service.query_failed(operation_id, code, msg, state)

    output_rows = [[0, "ok"]]
    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    return response


@app.post("/api/v1/test/metrics")
def fetch_metrics():
    """
    Intended to be used from the Streamlit application, this gets called through a
    Snowflake function.
    """
    metrics = MetricsService.fetch_metrics()

    output_rows = [[0, json.dumps(metrics)]]
    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    return response


enable_tcp_keep_alive()
service.start()

if __name__ == "__main__":
    # only used for local development, when gunicorn is not used
    app.run(host=SERVICE_HOST, port=int(SERVICE_PORT))
