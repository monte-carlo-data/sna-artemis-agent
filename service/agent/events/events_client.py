import datetime
import logging
from threading import Thread, Condition
from typing import Dict, Callable, Optional

from agent.events.heartbeat_checker import HeartbeatChecker
from agent.events.receiver_factory import ReceiverFactory

logger = logging.getLogger(__name__)


class EventsClient:
    """
    Client that abstract the underlying technology used to receive events from the backend,
    it could use a polling approach or a push approach like websockets or SSE.
    It delegates the creation of the receiver (the actual implementation that generates events)
    to the `ReceiverFactory`, which uses `SSE` by default.
    There are two event types that are handled here and not forwarded to the event handler:
    - `welcome`: sent as the first message after the connection is established
    - `heartbeat`: sent periodically by the server (by default every minute) to keep the connection
        alive. This class will re-establish the connection if we don't receive a heartbeat after
        2 minutes.
    """

    def __init__(
        self,
        receiver_factory: ReceiverFactory,
        base_url: str,
        handler: Callable[[Dict], None],
        heartbeat_checker: Optional[HeartbeatChecker] = None,
    ):
        self._receiver_factory = receiver_factory
        self._base_url = base_url
        self._event_handler = handler
        self._stopped = False
        self._receiver = self._receiver_factory.create_receiver(
            base_url=base_url,
            handler=self._event_received,
        )
        self._heartbeat_checker = heartbeat_checker or HeartbeatChecker(self._reconnect)

    @property
    def event_handler(self) -> Callable[[Dict], None]:
        return self._event_handler

    @event_handler.setter
    def event_handler(self, handler: Callable[[Dict], None]):
        self._event_handler = handler

    def start(self):
        self._receiver.start()
        self._heartbeat_checker.start()

    def stop(self):
        self._stopped = True
        self._heartbeat_checker.stop()
        self._receiver.stop()

    def _reconnect(self):
        self._receiver.stop()
        self._receiver = self._receiver_factory.create_receiver(
            base_url=self._base_url,
            handler=self._event_received,
        )
        self._receiver.start()

    def _event_received(self, event: Dict):
        event_type = event.get("type")
        if event_type in ("heartbeat", "welcome"):
            logger.info(f"{event_type}: {event.get('ts') or event.get('heartbeat')}")
            if event_type == "heartbeat":
                self._heartbeat_checker.heartbeat_received()
        else:
            self._event_handler(event)
