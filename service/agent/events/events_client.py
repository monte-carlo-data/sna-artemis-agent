import logging
from typing import Dict, Callable, Optional

from agent.events.base_receiver import BaseReceiver
from agent.events.heartbeat_checker import HeartbeatChecker

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
        receiver: BaseReceiver,
        heartbeat_checker: Optional[HeartbeatChecker] = None,
    ):
        self._receiver = receiver
        self._stopped = True
        self._heartbeat_checker = heartbeat_checker or HeartbeatChecker(self._reconnect)
        self._event_handler: Optional[Callable[[Dict], None]] = None

    def start(self, handler: Callable[[Dict], None]):
        self._event_handler = handler
        self._stopped = False
        self._receiver.start(
            handler=self._event_received,
            connected_handler=self._receiver_connected,
            disconnected_handler=self._receiver_disconnected,
        )

    def stop(self):
        self._stopped = True
        self._event_handler = None
        self._receiver.stop()

    def _reconnect(self):
        self._receiver.restart()

    def _event_received(self, event: Dict):
        event_type = event.get("type")
        if event_type == "heartbeat":
            logger.info(f"heartbeat: {event.get('ts')}")
            self._heartbeat_checker.heartbeat_received()
        elif event_type == "welcome":
            logger.info(f"{event_type}: agent_id={event.get('agent_id')}")
        elif self._event_handler:
            self._event_handler(event)

    def _receiver_connected(self):
        self._heartbeat_checker.start()

    def _receiver_disconnected(self):
        self._heartbeat_checker.stop()
