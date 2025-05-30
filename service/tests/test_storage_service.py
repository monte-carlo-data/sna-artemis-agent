import base64
import contextlib
from copy import deepcopy
from typing import Optional, Dict, Any
from unittest import TestCase
from unittest.mock import create_autospec, patch, Mock, mock_open

from agent.events.ack_sender import AckSender
from agent.events.base_receiver import BaseReceiver
from agent.events.events_client import EventsClient
from agent.events.heartbeat_checker import HeartbeatChecker
from agent.sna.config.config_manager import ConfigurationManager
from agent.sna.config.local_config import LocalConfig
from agent.sna.operations_runner import OperationsRunner
from agent.sna.queries_runner import QueriesRunner
from agent.sna.queries_service import QueriesService
from agent.sna.results_publisher import ResultsPublisher
from agent.sna.sna_service import SnaService
from agent.sna.timer_service import TimerService
from agent.storage.stage_reader_writer import StageReaderWriter
from agent.storage.storage_service import StorageService
from agent.utils.serde import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
)

_TEST_CONTENTS = "1234567890"
_WRITE_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "type": "storage_write",
        "key": "test/test.json",
        "obj_to_write": _TEST_CONTENTS,
    },
    "path": "/api/v1/agent/execute/storage/write",
}
_READ_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "type": "storage_read",
        "key": "test/test.json",
    },
    "path": "/api/v1/agent/execute/storage/read",
}
_GENERATE_PRE_SIGNED_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "type": "storage_generate_presigned_url",
        "key": "test/test.json",
    },
    "path": "/api/v1/agent/execute/storage/pre_signed",
}
_IS_BUCKET_PRIVATE_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "type": "storage_is_bucket_private",
    },
    "path": "/api/v1/agent/execute/storage/private_bucket",
}


class StorageServiceTests(TestCase):
    def setUp(self):
        self._mock_queries_runner = create_autospec(QueriesRunner)
        self._mock_ops_runner = create_autospec(OperationsRunner)
        self._mock_results_publisher = create_autospec(ResultsPublisher)
        self._events_client = EventsClient(
            receiver=create_autospec(BaseReceiver),
            heartbeat_checker=create_autospec(HeartbeatChecker),
        )
        self._queries_service = create_autospec(QueriesService)
        self._config_manager = ConfigurationManager(persistence=LocalConfig())
        self._storage_client = StageReaderWriter(
            stage_name="test.test_stage",
            local=False,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
        )
        self._storage_service = StorageService(
            client=self._storage_client,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
        )
        self._ack_sender = create_autospec(AckSender)
        self._logs_sender = create_autospec(TimerService)
        self._service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=self._events_client,
            storage_service=self._storage_service,
            ack_sender=self._ack_sender,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
            logs_sender=self._logs_sender,
        )
        self._service.start()

    @patch.object(StageReaderWriter, "_temp_location")
    @patch.object(SnaService, "_schedule_push_results")
    def test_write(
        self,
        mock_push_results: Mock,
        mock_temp_location: Mock,
    ):
        self._queries_service.run_query_and_fetch_all.return_value = [], []

        @contextlib.contextmanager
        def _mock_temp_location(file_name: str, contents: Optional[bytes] = None):
            yield "/tmp/test.json"

        mock_temp_location.side_effect = _mock_temp_location

        self._execute_storage_operation(_WRITE_OPERATION)
        expected_query = "PUT file:///tmp/test.json @test.test_stage/mcd/test/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        self._queries_service.run_query_and_fetch_all.assert_called_once_with(
            expected_query
        )
        mock_push_results.assert_called_once_with("1234", {ATTRIBUTE_NAME_RESULT: {}})

    @patch.object(StageReaderWriter, "_temp_directory")
    @patch.object(SnaService, "_schedule_push_results")
    @patch("os.remove")
    def test_read(
        self,
        mock_remove: Mock,
        mock_push_results: Mock,
        mock_temp_directory: Mock,
    ):
        self._queries_service.run_query_and_fetch_all.return_value = [], []

        @contextlib.contextmanager
        def _mock_temp_directory():
            yield "/tmp"

        mock_read_data = mock_open(read_data=_TEST_CONTENTS.encode("utf-8"))
        mock_temp_directory.side_effect = _mock_temp_directory

        with patch("builtins.open", mock_read_data):
            self._execute_storage_operation(_READ_OPERATION)
        expected_query = "GET @test.test_stage/mcd/test/test.json file:///tmp"
        self._queries_service.run_query_and_fetch_all.assert_called_once_with(
            expected_query
        )
        mock_read_data.assert_called_once_with("/tmp/test.json", "rb")
        mock_remove.assert_called_once_with("/tmp/test.json")
        mock_push_results.assert_called_once_with(
            "1234",
            {
                ATTRIBUTE_NAME_RESULT: {
                    "__type__": "bytes",
                    "__data__": base64.b64encode(_TEST_CONTENTS.encode("utf-8")).decode(
                        "utf-8"
                    ),
                },
            },
        )

    @patch.object(SnaService, "_schedule_push_results")
    def test_generate_pre_signed_url(
        self,
        mock_push_results: Mock,
    ):
        url = "https://test.com"
        self._queries_service.run_query_and_fetch_all.return_value = [[url]], []

        self._execute_storage_operation(_GENERATE_PRE_SIGNED_OPERATION)
        expected_query = "CALL CORE.EXECUTE_QUERY(?)"
        expected_query_param = (
            "CALL GET_PRESIGNED_URL(@test.test_stage, 'mcd/test/test.json', 300.0)"
        )
        self._queries_service.run_query_and_fetch_all.assert_called_once_with(
            expected_query, [expected_query_param]
        )
        mock_push_results.assert_called_once_with(
            "1234", {ATTRIBUTE_NAME_RESULT: "https://test.com"}
        )

    @patch.object(SnaService, "_schedule_push_results")
    def test_is_bucket_private(self, mock_push_results: Mock):
        self._execute_storage_operation(_IS_BUCKET_PRIVATE_OPERATION)
        mock_push_results.assert_called_once_with("1234", {ATTRIBUTE_NAME_RESULT: True})

    @patch.object(SnaService, "_schedule_push_results")
    def test_invalid_operation(self, mock_push_results: Mock):
        operation = deepcopy(_IS_BUCKET_PRIVATE_OPERATION)
        operation["operation"]["type"] = "invalid"
        self._execute_storage_operation(operation)
        mock_push_results.assert_called_once_with(
            "1234",
            {
                ATTRIBUTE_NAME_ERROR: "Invalid operation type: invalid",
            },
        )

    def _execute_storage_operation(self, event: Dict[str, Any]):
        self._service._execute_storage_operation(event["operation_id"], event)
