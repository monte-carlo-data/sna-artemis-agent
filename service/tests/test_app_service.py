import base64
import gzip
import json
from copy import deepcopy
from typing import Dict
from unittest import TestCase
from unittest.mock import create_autospec, patch, ANY, Mock

from apollo.egress.agent.events.base_receiver import BaseReceiver
from apollo.egress.agent.events.events_client import EventsClient
from apollo.egress.agent.events.heartbeat_checker import HeartbeatChecker
from apollo.egress.agent.config.config_manager import ConfigurationManager
from apollo.egress.agent.config.config_persistence import ConfigurationPersistence
from apollo.egress.agent.service.login_token_provider import (
    LocalLoginTokenProvider,
    LoginTokenProvider,
)
from apollo.egress.agent.service.operation_result import (
    OperationAttributes,
    AgentOperationResult,
)
from apollo.egress.agent.service.operations_runner import OperationsRunner, Operation
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_ATTRS,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_TRACE_ID,
)
from apollo.egress.agent.service.timer_service import TimerService
from apollo.egress.agent.utils.queue_async_processor import T
from apollo.egress.agent.utils.utils import BACKEND_SERVICE_URL, X_MCD_ID, X_MCD_TOKEN

from agent.sna.queries_runner import QueriesRunner
from agent.sna.queries_service import QueriesService
from apollo.egress.agent.service.results_publisher import ResultsPublisher
from agent.sna.sf_query import SnowflakeQuery
from agent.sna.sna_service import SnaService
from agent.storage.storage_service import StorageService

_QUERY_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "type": "snowflake_query",
        "query": "SELECT * FROM table",
        "trace_id": "5432",
        "compress_response_file": False,
        "response_size_limit_bytes": 100000,
    },
    "path": "/api/v1/agent/execute/snowflake/query",
}

_HEALTH_OPERATION = {
    "operation_id": "1234",
    "operation": {
        "trace_id": "5432",
    },
    "path": "/api/v1/test/health",
}


class AppServiceTests(TestCase):
    def setUp(self):
        # Stub the operations poller so its background thread doesn't run during unit tests —
        # it would poll the backend over HTTP and trip backpressure checks on mocked runners.
        poller_patcher = patch(
            "apollo.egress.agent.service.base_egress_service.OperationsPoller"
        )
        poller_patcher.start()
        self.addCleanup(poller_patcher.stop)
        self._mock_events_client = create_autospec(EventsClient)
        self._mock_queries_runner = create_autospec(QueriesRunner)
        self._mock_ops_runner = Mock()
        self._mock_ops_runner.queue_depth.return_value = 0
        self._mock_ops_runner.thread_count = 1
        self._mock_results_publisher = create_autospec(ResultsPublisher)
        self._queries_service = create_autospec(QueriesService)
        self._config_persistence = create_autospec(ConfigurationPersistence)
        self._config_manager = ConfigurationManager(
            persistence=self._config_persistence
        )
        self._logs_sender = create_autospec(TimerService)
        self._service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=self._mock_events_client,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
            logs_sender=self._logs_sender,
            login_token_provider=LocalLoginTokenProvider(),
        )

    def test_service_start_stop(self):
        self._service._sse_enabled = True  # Enable SSE for this test
        self._service.start()
        self._mock_queries_runner.start.assert_called_once()
        self._mock_results_publisher.start.assert_called_once()
        self._mock_events_client.start.assert_called_once()

        self._service.stop()
        self._mock_queries_runner.stop.assert_called_once()
        self._mock_results_publisher.stop.assert_called_once()
        self._mock_events_client.stop.assert_called_once()

    @patch("requests.put")
    def test_query_execution(self, mock_requests_put: Mock):
        events_client = create_autospec(EventsClient)
        ops_runner = ImmediateOpsRunner(lambda op: None)
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=events_client,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
            logs_sender=self._logs_sender,
            login_token_provider=LocalLoginTokenProvider(),
        )
        ops_runner._ops_handler = service._execute_scheduled_operation
        service.start()
        # Simulate pull model - call _handle_polled_operation directly
        service._handle_polled_operation(
            _QUERY_OPERATION["path"],
            _QUERY_OPERATION["operation_id"],
            _QUERY_OPERATION,
        )
        operation_attrs = OperationAttributes(
            operation_id="1234",
            trace_id="5432",
            compress_response_file=False,
            response_size_limit_bytes=100000,
        )
        self._mock_queries_runner.schedule.assert_called_once_with(
            SnowflakeQuery(
                operation_id="1234",
                query="SELECT * FROM table",
                timeout=ANY,
                operation_attrs=operation_attrs,
            )
        )

        service.query_completed(operation_attrs.to_json(), "5678")
        self._mock_results_publisher.schedule_push_query_results.assert_called_once_with(
            "1234", "5678", operation_attrs
        )
        # now simulate the publisher called its handler
        query_result = {
            ATTRIBUTE_NAME_RESULT: {
                "all_results": [],
                "description": [],
                "rowcount": 0,
            },
        }
        self._queries_service.result_for_query.return_value = query_result
        service._push_results_for_query("1234", "5678", operation_attrs)
        mock_requests_put.assert_called_once_with(
            ANY,
            data=ANY,
            headers={
                "Content-Type": "application/json",
                "x-mcd-id": "local-token-id",
                "x-mcd-token": "local-token-secret",
                "x-mcd-agent-instance-id": ANY,
            },
            timeout=60,
        )
        url = mock_requests_put.call_args[0][0]
        self.assertTrue(url.endswith("/api/v1/agent/operations/1234/result"))
        sent_data = mock_requests_put.call_args[1]["data"]
        sent_dict = json.loads(sent_data)
        self.assertEqual({"result": query_result}, sent_dict)

        # test query failed flow
        service.query_failed(
            operation_attrs.to_json(), 1678, "error msg", "error state"
        )
        self._mock_results_publisher.schedule_push_results.assert_called_once_with(
            operation_id="1234",
            result={
                ATTRIBUTE_NAME_ERROR: "error msg",
                ATTRIBUTE_NAME_ERROR_ATTRS: {"errno": 1678, "sqlstate": "error state"},
                ATTRIBUTE_NAME_ERROR_TYPE: "DatabaseError",
            },
            operation_attrs=operation_attrs,
        )

        # test query failed with programming error
        self._mock_results_publisher.reset_mock()
        service.query_failed(
            operation_attrs.to_json(), 2043, "error msg", "error state"
        )
        self._mock_results_publisher.schedule_push_results.assert_called_once_with(
            operation_id="1234",
            result={
                ATTRIBUTE_NAME_ERROR: "error msg",
                ATTRIBUTE_NAME_ERROR_ATTRS: {"errno": 2043, "sqlstate": "error state"},
                ATTRIBUTE_NAME_ERROR_TYPE: "ProgrammingError",
            },
            operation_attrs=operation_attrs,
        )

    @patch("requests.put")
    def test_query_execution_pre_signed_url(self, mock_requests_put: Mock):
        # Configure mock to return a proper response without piggybacked operation
        mock_response = Mock()
        mock_response.json.return_value = {"operation_id": "1234"}
        mock_requests_put.return_value = mock_response

        events_client = create_autospec(EventsClient)
        storage = create_autospec(StorageService)
        ops_runner = ImmediateOpsRunner(lambda op: None)
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=events_client,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
            storage_service=storage,
            logs_sender=self._logs_sender,
            login_token_provider=LocalLoginTokenProvider(),
            enable_pre_signed_urls=True,
        )
        ops_runner._ops_handler = service._execute_scheduled_operation
        service.start()
        query_operation = deepcopy(_QUERY_OPERATION)
        query_operation["operation"]["response_size_limit_bytes"] = 1
        # Simulate pull model - call _handle_polled_operation directly
        service._handle_polled_operation(
            query_operation["path"],
            query_operation["operation_id"],
            query_operation,
        )
        operation_attrs = OperationAttributes(
            operation_id="1234",
            trace_id="5432",
            compress_response_file=False,
            response_size_limit_bytes=1,
        )
        self._mock_queries_runner.schedule.assert_called_once_with(
            SnowflakeQuery(
                operation_id="1234",
                query="SELECT * FROM table",
                timeout=ANY,
                operation_attrs=operation_attrs,
            )
        )

        service.query_completed(operation_attrs.to_json(), "5678")
        self._mock_results_publisher.schedule_push_query_results.assert_called_once_with(
            "1234", "5678", operation_attrs
        )
        large_result = {
            ATTRIBUTE_NAME_RESULT: {
                "big_result": True,
            }
        }
        storage.generate_presigned_url.return_value = "http://presigned.url"
        self._config_persistence.get_value.return_value = None
        service._push_results(
            AgentOperationResult(
                operation_id="1234",
                result=deepcopy(large_result),
                operation_attrs=operation_attrs,
            ),
        )
        storage.write.assert_called_once_with(
            key="responses/5432",
            obj_to_write=ANY,
        )
        saved_data = storage.write.call_args[1]["obj_to_write"]
        storage.generate_presigned_url.assert_called_once_with("responses/5432", 3600)
        self.assertEqual(
            {
                **large_result,
                ATTRIBUTE_NAME_TRACE_ID: "5432",
            },
            json.loads(saved_data),
        )
        mock_requests_put.assert_called_once()

        # test compression now
        storage.reset_mock()

        operation_attrs = OperationAttributes(
            operation_id="1234",
            trace_id="5432",
            compress_response_file=True,
            response_size_limit_bytes=1,
        )
        service._push_results(
            AgentOperationResult(
                operation_id="1234",
                result=deepcopy(large_result),
                operation_attrs=operation_attrs,
            ),
        )
        storage.write.assert_called_once_with(
            key="responses/5432",
            obj_to_write=ANY,
        )
        compressed_data = storage.write.call_args[1]["obj_to_write"]
        saved_dict = json.loads(gzip.decompress(compressed_data).decode())
        self.assertEqual(
            {
                **large_result,
                ATTRIBUTE_NAME_TRACE_ID: "5432",
            },
            saved_dict,
        )
        storage.generate_presigned_url.assert_called_once_with("responses/5432", 3600)

    def test_health_operation(self):
        events_client = create_autospec(EventsClient)
        self._config_persistence.get_all_values.return_value = {
            "setting_1": "value_1",
            "setting_2": "value_2",
        }
        service = SnaService(
            queries_runner=self._mock_queries_runner,
            ops_runner=self._mock_ops_runner,
            results_publisher=self._mock_results_publisher,
            events_client=events_client,
            queries_service=self._queries_service,
            config_manager=self._config_manager,
            logs_sender=self._logs_sender,
            login_token_provider=LocalLoginTokenProvider(),
        )
        service.start()
        # Simulate pull model - call _handle_polled_operation directly
        service._handle_polled_operation(
            _HEALTH_OPERATION["path"],
            _HEALTH_OPERATION["operation_id"],
            _HEALTH_OPERATION,
        )
        operation = Operation(
            operation_id="1234",
            event=_HEALTH_OPERATION,
        )
        self._mock_ops_runner.schedule.assert_called_once_with(operation)

        # now simulate the operations runner executed the operation
        self._service._execute_scheduled_operation(operation)
        health_info = service.health_information(
            _HEALTH_OPERATION["operation"]["trace_id"]
        )
        self.assertEqual(
            health_info["parameters"], {"setting_1": "value_1", "setting_2": "value_2"}
        )
        self._mock_results_publisher.schedule_push_results.assert_called_once_with(
            operation_id="1234",
            result=health_info,
            operation_attrs=None,
        )

    def test_reachability_test(self):
        health_response = {"status": "ok"}
        with patch("requests.request") as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.json.return_value = health_response
            result = self._service.run_reachability_test(trace_id="1234")
            mock_request.assert_called_once_with(
                method="GET",
                url=BACKEND_SERVICE_URL + "/api/v1/test/ping?trace_id=1234",
                json=None,
                headers={
                    "x-mcd-id": "local-token-id",
                    "x-mcd-token": "local-token-secret",
                    "x-mcd-agent-instance-id": ANY,
                },
                timeout=None,
            )
            self.assertEqual(health_response, result)

    def test_reachability_test_failed(self):
        with patch("requests.request") as mock_request:
            mock_request.return_value.raise_for_status.side_effect = Exception(
                "ping failed"
            )
            result = self._service.run_reachability_test()
            self.assertEqual({"error": "ping failed"}, result)


def _mcd_id_for_tenant(tenant: str) -> str:
    """Build a realistic mcd_id whose base64 payload decodes to ``v1+<tenant>``."""
    encoded = base64.b64encode(f"v1+{tenant}".encode("utf-8")).decode("utf-8")
    return f"test-id+{encoded}"


class _StubLoginTokenProvider(LoginTokenProvider):
    """Test double that returns a fixed mcd_id/mcd_token without touching the filesystem."""

    def __init__(self, mcd_id: str, mcd_token: str = "test-token-secret"):
        self._mcd_id = mcd_id
        self._mcd_token = mcd_token

    def get_token(self) -> Dict[str, str]:
        return {X_MCD_ID: self._mcd_id, X_MCD_TOKEN: self._mcd_token}


class BackendUrlResolutionWiringTests(TestCase):
    """Covers the glue in ``SnaService.__init__`` that reads the token, calls
    the resolver, and routes the resulting URL into ``BaseEgressAgentService``.

    The helpers (``parse_tenant_from_id``, ``resolve_backend_url``) are tested
    in their own modules; these tests pin down the single point that decides
    which backend this agent talks to.
    """

    def _build_service(self, login_token_provider: LoginTokenProvider) -> SnaService:
        config_persistence = create_autospec(ConfigurationPersistence)
        return SnaService(
            queries_runner=create_autospec(QueriesRunner),
            ops_runner=Mock(queue_depth=Mock(return_value=0), thread_count=1),
            results_publisher=create_autospec(ResultsPublisher),
            events_client=create_autospec(EventsClient),
            queries_service=create_autospec(QueriesService),
            config_manager=ConfigurationManager(persistence=config_persistence),
            logs_sender=create_autospec(TimerService),
            login_token_provider=login_token_provider,
        )

    def test_service_uses_eu_url_for_eu_tenant(self):
        service = self._build_service(
            _StubLoginTokenProvider(_mcd_id_for_tenant("eu1")),
        )
        expected = BACKEND_SERVICE_URL.replace("artemis.", "artemis.eu1.", 1)
        self.assertEqual(expected, service._backend_service_url)

    def test_service_uses_fallback_url_for_us1_tenant(self):
        service = self._build_service(
            _StubLoginTokenProvider(_mcd_id_for_tenant("us1")),
        )
        self.assertEqual(BACKEND_SERVICE_URL, service._backend_service_url)

    def test_service_aborts_on_invalid_tenant(self):
        # "bad/tenant" has a `/` which is rejected by the tenant-format check
        # in resolve_backend_url, so SnaService construction must raise.
        with self.assertRaises(ValueError):
            self._build_service(
                _StubLoginTokenProvider(_mcd_id_for_tenant("bad/tenant")),
            )


class ImmediateOpsRunner(OperationsRunner):
    def start(self):
        pass  # skip starting thread

    def schedule(self, o: Operation):
        self._invoke_handler("immediate", o)
