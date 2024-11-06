from collections.abc import Callable
from datetime import timedelta
from typing import Dict, Any, Optional, Union

from agent.storage.base_storage_client import BaseStorageClient
from agent.storage.stage_reader_writer import StageReaderWriter
from agent.utils.serde import AgentSerializer


class StorageService:
    def __init__(self, client: Optional[BaseStorageClient] = None):
        self._client = client or StageReaderWriter()
        self._mapping: Dict[str, Callable[[Dict[str, Any]], Any]] = {
            "storage_read": self._read,
            "storage_read_json": self._read_json,
            "storage_write": self._write,
            "storage_delete": self._delete,
            "storage_generate_presigned_url": self._pre_signed_url,
            "storage_is_bucket_private": self._is_bucket_private,
        }

    def execute_operation(self, event: Dict[str, Any]) -> Any:
        operation = event.get("operation", {})
        operation_type = operation.get("type")
        method = self._mapping.get(operation_type) if operation_type else None
        if not method:
            raise ValueError(f"Invalid operation type: {operation_type}")
        return method(operation)

    def _read(self, operation: Dict[str, Any]) -> Any:
        contents = self._client.read(
            key=self._get_key(operation),
            decompress=operation.get("decompress", False),
            encoding=operation.get("encoding"),
        )
        if isinstance(contents, bytes):
            return AgentSerializer.serialize(contents)
        return contents

    def _read_json(self, operation: Dict[str, Any]) -> Any:
        return self._client.read_json(key=self._get_key(operation))

    def _write(self, operation: Dict[str, Any]) -> Any:
        obj_to_write = operation.get("obj_to_write")
        if isinstance(obj_to_write, str) or isinstance(obj_to_write, bytes):
            self._client.write(key=self._get_key(operation), obj_to_write=obj_to_write)
        elif obj_to_write:
            raise ValueError(
                f"Invalid type for obj_to_write parameter: {type(obj_to_write).__name__}"
            )
        return {}

    def _delete(self, operation: Dict[str, Any]) -> Any:
        self._client.delete(key=self._get_key(operation))
        return {}

    def _pre_signed_url(self, operation: Dict[str, Any]) -> Any:
        return self._client.generate_presigned_url(
            key=self._get_key(operation),
            expiration=timedelta(seconds=operation.get("expiration", 300)),
        )

    def _is_bucket_private(self, operation: Dict[str, Any]) -> Any:
        return self._client.is_bucket_private()

    @staticmethod
    def _get_key(operation: Dict[str, Any]) -> str:
        key = operation.get("key")
        if not key:
            raise ValueError("Key is required")
        return key
