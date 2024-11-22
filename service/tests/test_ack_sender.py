from unittest import TestCase
from unittest.mock import patch, Mock

from agent.events.ack_sender import AckSender


class AckSenderTests(TestCase):
    def setUp(self):
        self._ack_sender = AckSender(interval_seconds=10)

    @patch("time.time")
    def test_schedule_ack(self, mock_time: Mock):
        now = 100
        mock_time.return_value = now
        self._ack_sender.schedule_ack("1234")
        self.assertEqual(1, len(self._ack_sender._queue))
        self.assertEqual("1234", self._ack_sender._queue[0].operation_id)
        self.assertEqual(now + 10, self._ack_sender._queue[0].scheduled_time)
        self.assertEqual("1234", self._ack_sender._mapping["1234"].operation_id)
        self.assertFalse(self._ack_sender._mapping["1234"].completed)

    @patch("time.time")
    def test_send_ack(self, mock_time: Mock):
        now = 100
        mock_time.return_value = now
        self._ack_sender.schedule_ack("1234")

        self._ack_sender._handler = Mock()
        now = 120
        mock_time.return_value = now
        self._ack_sender._running = True
        self._ack_sender._run_once()
        self._ack_sender._handler.assert_called_once_with("1234")

    @patch("time.time")
    def test_no_ack_for_completed_task(self, mock_time: Mock):
        now = 100
        mock_time.return_value = now
        self._ack_sender.schedule_ack("1234")
        self._ack_sender.operation_completed("1234")

        self._ack_sender._handler = Mock()
        now = 120
        mock_time.return_value = now
        self._ack_sender._running = True
        self._ack_sender._run_once()
        self._ack_sender._handler.assert_not_called()
