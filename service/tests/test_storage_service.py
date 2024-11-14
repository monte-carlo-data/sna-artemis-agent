import base64
import contextlib
from copy import deepcopy
from typing import Optional
from unittest import TestCase
from unittest.mock import create_autospec, patch, Mock, mock_open

from agent.backend.backend_client import BackendClient
from agent.events.base_receiver import BaseReceiver
from agent.events.events_client import EventsClient
from agent.events.heartbeat_checker import HeartbeatChecker
from agent.sna.queries_runner import QueriesRunner
from agent.sna.results_publisher import ResultsPublisher
from agent.sna.sf_client import SnowflakeClient
from agent.sna.sna_service import SnaService
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
        self._mock_results_publisher = create_autospec(ResultsPublisher)
        self._events_client = EventsClient(
            receiver=create_autospec(BaseReceiver),
            heartbeat_checker=create_autospec(HeartbeatChecker),
        )
        self._storage_client = StageReaderWriter(stage_name="test.test_stage")
        self._storage_service = StorageService(client=self._storage_client)
        self._service = SnaService(
            queries_runner=self._mock_queries_runner,
            results_publisher=self._mock_results_publisher,
            events_client=self._events_client,
            storage_service=self._storage_service,
        )
        self._service.start()

    @patch.object(SnowflakeClient, "run_query_and_fetch_all")
    @patch.object(StageReaderWriter, "_temp_location")
    @patch.object(BackendClient, "push_results")
    def test_write(
        self,
        mock_push_results: Mock,
        mock_temp_location: Mock,
        mock_run_query: Mock,
    ):
        mock_run_query.return_value = [], []

        @contextlib.contextmanager
        def _mock_temp_location(file_name: str, contents: Optional[bytes] = None):
            yield "/tmp/test.json"

        mock_temp_location.side_effect = _mock_temp_location

        self._events_client._event_received(_WRITE_OPERATION)
        expected_query = "PUT file:///tmp/test.json @test.test_stage/mcd/test/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        mock_run_query.assert_called_once_with(expected_query)
        mock_push_results.assert_called_once_with("1234", {ATTRIBUTE_NAME_RESULT: {}})

    @patch.object(SnowflakeClient, "run_query_and_fetch_all")
    @patch.object(StageReaderWriter, "_temp_directory")
    @patch.object(BackendClient, "push_results")
    @patch("os.remove")
    def test_read(
        self,
        mock_remove: Mock,
        mock_push_results: Mock,
        mock_temp_directory: Mock,
        mock_run_query: Mock,
    ):
        mock_run_query.return_value = [], []

        @contextlib.contextmanager
        def _mock_temp_directory():
            yield "/tmp"

        mock_read_data = mock_open(read_data=_TEST_CONTENTS.encode("utf-8"))
        mock_temp_directory.side_effect = _mock_temp_directory

        with patch("builtins.open", mock_read_data):
            self._events_client._event_received(_READ_OPERATION)
        expected_query = "GET @test.test_stage/mcd/test/test.json file:///tmp"
        mock_run_query.assert_called_once_with(expected_query)
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

    @patch.object(SnowflakeClient, "run_query_and_fetch_all")
    @patch.object(BackendClient, "push_results")
    def test_generate_pre_signed_url(
        self,
        mock_push_results: Mock,
        mock_run_query: Mock,
    ):
        url = "https://test.com"
        mock_run_query.return_value = [[url]], []

        self._events_client._event_received(_GENERATE_PRE_SIGNED_OPERATION)
        expected_query = "CALL mcd_agent.core.execute_query(?)"
        expected_query_param = (
            "CALL GET_PRESIGNED_URL(@test.test_stage, 'mcd/test/test.json', 300.0)"
        )
        mock_run_query.assert_called_once_with(expected_query, [expected_query_param])
        mock_push_results.assert_called_once_with(
            "1234", {ATTRIBUTE_NAME_RESULT: "https://test.com"}
        )

    @patch.object(BackendClient, "push_results")
    def test_is_bucket_private(self, mock_push_results: Mock):
        self._events_client._event_received(_IS_BUCKET_PRIVATE_OPERATION)
        mock_push_results.assert_called_once_with("1234", {ATTRIBUTE_NAME_RESULT: True})

    @patch.object(BackendClient, "push_results")
    def test_invalid_operation(self, mock_push_results: Mock):
        operation = deepcopy(_IS_BUCKET_PRIVATE_OPERATION)
        operation["operation"]["type"] = "invalid"
        self._events_client._event_received(operation)
        mock_push_results.assert_called_once_with(
            "1234",
            {
                ATTRIBUTE_NAME_ERROR: "Invalid operation type: invalid",
            },
        )
