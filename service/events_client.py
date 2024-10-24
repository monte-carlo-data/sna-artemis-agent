import datetime
import json
import time
from threading import Thread, Condition
from typing import Optional, Dict, Callable
from urllib.parse import urljoin
from utils import get_mc_login_token, get_logger

import sseclient

logger = get_logger(__name__)


class EventsClient:
    def __init__(self, base_url: str, agent_id: str, handler: Callable[[Dict], None]):
        self._base_url = base_url
        self._agent_id = agent_id
        self._event_handler = handler
        self._stopped = False
        self._receiver = SSEClientReceiver(
            base_url=base_url,
            agent_id=agent_id,
            handler=self._event_received,
        )
        self._heartbeat_condition = Condition()
        self._last_heartbeat = datetime.datetime.now()

    def start(self):
        self._receiver.start()
        th = Thread(target=self._run_heartbeat_checker)
        th.start()

    def stop(self):
        self._stopped = True
        self._heartbeat_received()  # wake up the checker to stop running
        self._receiver.stop()

    def _reconnect(self):
        self._receiver.stop()
        self._receiver = SSEClientReceiver(
            base_url=self._base_url,
            agent_id=self._agent_id,
            handler=self._event_received,
        )
        self._receiver.start()

    def _event_received(self, event: Dict):
        event_type = event.get("type")
        if event_type in ("heartbeat", "welcome") or event.get("heartbeat"):
            logger.info(f"{event_type}: {event.get('ts') or event.get('heartbeat')}")
            if event_type == "heartbeat":
                self._heartbeat_received()
        else:
            self._event_handler(event)

    def _run_heartbeat_checker(self):
        logger.info("Heartbeat monitor started")
        while not self._stopped:
            with self._heartbeat_condition:
                self._heartbeat_condition.wait(timeout=60)
                elapsed_time = datetime.datetime.now() - self._last_heartbeat
            if self._stopped:
                break
            if elapsed_time.total_seconds() > 90:
                logger.error("Heartbeat timeout")
                self._reconnect()
        logger.info("Heartbeat monitor stopped")

    def _heartbeat_received(self):
        with self._heartbeat_condition:
            self._last_heartbeat = datetime.datetime.now()
            self._heartbeat_condition.notify()


class SSEClientReceiver:
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
                url = urljoin(self._base_url, f"/stream?channel=agents.input.{self._agent_id}")
                headers = {
                    "Accept": "text/event-stream",
                    "x-mcd-token": get_mc_login_token(),
                }
                self._sse_client = sseclient.SSEClient(url, headers=headers)
                for event in self._sse_client:
                    if self._stopped:
                        break
                    try:
                        event = json.loads(event.data)
                        if self._event_handler:
                            self._event_handler(event)
                    except Exception as parse_ex:
                        logger.debug(f"Failed to parse event: {parse_ex}, text: {event.data}")
            except Exception as ex:
                logger.error(f"Connection failed: {ex}")
                if not self._stopped:
                    time.sleep(5)
                    # TODO: exponential backoff
        logger.info("SSE Client stopped")
