import os
import signal
import sys

from flask import Flask
from flask import make_response
from flask import request

from agent.sna.sna_service import SnaService
from agent.utils.utils import get_logger, enable_tcp_keep_alive

SERVICE_HOST = os.getenv('SERVER_HOST', '0.0.0.0')
SERVICE_PORT = os.getenv('SERVER_PORT', 8080)

logger = get_logger(__name__)

app = Flask(__name__)
service = SnaService()


def handler(signum, frame):
    print("Signal handler called with signal", signum)
    service.stop()
    print("Signal handler completed")
    sys.exit(0)


signal.signal(signal.SIGINT, handler)


@app.get("/healthcheck")
def health_check():
    return "OK"


@app.post("/push_metrics")
def fetch_metrics():
    service.fetch_metrics()

    output_rows = [[0, "metrics pushed"]]
    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'
    return response


@app.post("/query_completed")
def query_completed():
    message = request.json
    logger.debug(f'Received query completed: {message}')

    if message is None or not message['data']:
        logger.info('Received empty message')
        return {}

    input_rows = message['data']
    if input_rows:
        op_id = input_rows[0][1]
        query_id = input_rows[0][2]
        logger.info(f"QUERY COMPLETED: op_id={op_id}, query_id={query_id}")
        service.query_completed(op_id, query_id)

    output_rows = [[0, "ok"]]
    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'
    return response


@app.post("/query_failed")
def query_failed():
    message = request.json
    logger.debug(f'Received query failed: {message}')

    if message is None or not message['data']:
        logger.info('Received empty message')
        return {}

    input_rows = message['data']
    if input_rows:
        operation_id = input_rows[0][1]
        code = input_rows[0][2]
        msg = input_rows[0][3]
        state = input_rows[0][4]
        service.query_failed(operation_id, code, msg, state)

    output_rows = [[0, "ok"]]
    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'
    return response


enable_tcp_keep_alive()


if __name__ == '__main__':
    app.run(host=SERVICE_HOST, port=SERVICE_PORT)
