import datetime
import logging
from threading import Thread, Condition
from typing import Dict, Callable, Optional

from agent.events.heartbeat_checker import HeartbeatChecker
from agent.events.receiver_factory import ReceiverFactory

logger = logging.getLogger(__name__)


class EventsClient:
    def __init__(
        self,
        receiver_factory: ReceiverFactory,
        base_url: str,
        agent_id: str,
        handler: Callable[[Dict], None],
        heartbeat_checker: Optional[HeartbeatChecker] = None,
    ):
        self._receiver_factory = receiver_factory
        self._base_url = base_url
        self._agent_id = agent_id
        self._event_handler = handler
        self._stopped = False
        self._receiver = self._receiver_factory.create_receiver(
            base_url=base_url,
            agent_id=agent_id,
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
            agent_id=self._agent_id,
            handler=self._event_received,
        )
        self._receiver.start()

    def _event_received(self, event: Dict):
        event_type = event.get("type")
        if event_type in ("heartbeat", "welcome") or event.get("heartbeat"):
            logger.info(f"{event_type}: {event.get('ts') or event.get('heartbeat')}")
            if event_type == "heartbeat":
                self._heartbeat_checker.heartbeat_received()
        else:
            self._event_handler(event)
