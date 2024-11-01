import json
import logging
import time
from threading import Thread
from typing import Callable, Dict, Optional
from urllib.parse import urljoin

import sseclient

from agent.events.receiver_factory import ReceiverFactory
from agent.utils.utils import get_mc_login_token

logger = logging.getLogger(__name__)


class SSEClientReceiverFactory(ReceiverFactory):
    def create_receiver(
        self, base_url: str, agent_id: str, handler: Callable[[Dict], None]
    ):
        return SSEClientReceiver(base_url, agent_id, handler)


class SSEClientReceiver:
    """
    Receiver that uses SSE (Server-sent events) to listen for events from the server and
    call the handler function when an event is received.
    """

    def __init__(
        self,
        base_url: str,
        agent_id: str,
        handler: Callable[[Dict], None],
    ):
        self._stopped = False
        self._base_url = base_url
        self._agent_id = agent_id
        self._sse_client: Optional[sseclient.SSEClient] = None
        self._event_handler: Optional[Callable[[Dict], None]] = handler

    def start(self):
        th = Thread(target=self._run_receiver)
        th.start()

    def stop(self):
        self._event_handler = None
        self._stopped = True

    def _run_receiver(self):
        while not self._stopped:
            try:
                logger.info("Connecting SSE Client ...")
                url = urljoin(
                    self._base_url, f"/stream?channel=agents.input.{self._agent_id}"
                )
                headers = {
                    "Accept": "text/event-stream",
                    **get_mc_login_token(),
                }
                self._sse_client = sseclient.SSEClient(url, headers=headers)
                for event in self._sse_client:
                    if self._stopped:
                        break
                    try:
                        event = json.loads(event.data)
                    except Exception as parse_ex:
                        logger.exception(
                            f"Failed to parse event: {parse_ex}, text: {event.data}"
                        )
                        continue
                    try:
                        if self._event_handler:
                            self._event_handler(event)
                    except Exception as parse_ex:
                        logger.exception(
                            f"Failed to process event: {parse_ex}, text: {event.data}"
                        )
            except Exception as ex:
                logger.error(f"Connection failed: {ex}")
                if not self._stopped:
                    time.sleep(5)
                    # TODO: exponential backoff
        logger.info("SSE Client stopped")
