import contextlib
import gzip
import os.path
import tempfile
from datetime import timedelta
from typing import Optional, Tuple, Union, List, Dict, Iterator
from uuid import uuid4

from snowflake.connector import OperationalError

from agent.sna.config.config_manager import ConfigurationManager
from agent.sna.config.config_keys import CONFIG_STAGE_NAME

from agent.sna.queries_service import QueriesService
from agent.storage.base_storage_client import BaseStorageClient
from agent.utils.utils import LOCAL

_DEFAULT_PREFIX = "mcd"
_SNOWFLAKE_ERROR_FILE_NOT_FOUND = 253006


class StageReaderWriter(BaseStorageClient):
    def __init__(
        self,
        config_manager: ConfigurationManager,
        queries_service: QueriesService,
        stage_name: Optional[str] = None,
        prefix: Optional[str] = _DEFAULT_PREFIX,
        local: bool = LOCAL,
    ):
        super().__init__(prefix=prefix)
        self._queries_service = queries_service
        self._stage_name = stage_name or os.getenv(
            CONFIG_STAGE_NAME,
            config_manager.get_str_value(
                CONFIG_STAGE_NAME, "mcd_agent.core.data_store"
            ),
        )
        self._local = local

    @property
    def bucket_name(self) -> str:
        return self._stage_name

    def write(self, key: str, obj_to_write: Union[bytes, str]) -> None:
        """
        Uses `PUT` to upload the file to the internal stage, the contents are first saved to
        a temporary file that gets deleted automatically.
        """
        folder, file_name = self._parse_key(key)
        bytes_to_write = (
            obj_to_write.encode("utf-8")
            if isinstance(obj_to_write, str)
            else obj_to_write
        )

        with self._temp_location(file_name, bytes_to_write) as tmp_location:
            put_query = (
                f"PUT file://{tmp_location} "
                f"@{self._stage_name}/{self._apply_prefix(folder)} "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            self._run_stage_query(put_query, "write", key)

    def read(
        self,
        key: str,
        decompress: Optional[bool] = False,
        encoding: Optional[str] = None,
    ) -> Union[bytes, str]:
        """
        Uses `GET` to download the file to a temporary location and return its contents.
        The file is deleted automatically.
        """
        _, file_name = self._parse_key(key)
        with self._temp_directory() as tmp_dir:
            get_query = (
                f"GET @{self._stage_name}/{self._apply_prefix(key)} file://{tmp_dir}"
            )
            self._run_stage_query(get_query, "read", key)

            tmp_location = os.path.join(tmp_dir, file_name)
            try:
                with open(tmp_location, "rb") as f:
                    content = f.read()
            finally:
                os.remove(tmp_location)

        if decompress and self._is_gzip(content):
            content = gzip.decompress(content)
        if encoding is not None:
            content = content.decode(encoding)
        return content

    def delete(self, key: str) -> None:
        """
        Uses `REMOVE` to delete the file.
        """
        delete_query = f"REMOVE @{self._stage_name}/{self._apply_prefix(key)}"
        self._run_stage_query(delete_query, "delete", key)

    def download_file(self, key: str, download_path: str) -> None:
        """
        Uses `GET` to download the file to the specified location, `GET` works by downloading
        the file with the same name to a local folder, then we rename it to match the
        requested file name.
        """
        download_dir, download_file_name = os.path.split(download_path)
        _, file_name = self._parse_key(key)

        get_query = (
            f"GET @{self._stage_name}/{self._apply_prefix(key)} file://{download_dir}"
        )
        self._run_stage_query(get_query, "download", key)

        # files are downloaded with the name of the key, so we might need to rename it
        if file_name != download_file_name:
            os.rename(os.path.join(download_dir, file_name), download_path)

    def upload_file(self, key: str, local_file_path: str) -> None:
        """
        Uses `PUT` to upload the file to the internal stage.
        """
        put_query = (
            f"PUT file://{local_file_path} @{self._stage_name}/{self._apply_prefix(key)} "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        self._run_stage_query(put_query, "upload", key)

    def read_many_json(self, prefix: str) -> Dict:
        """
        Not implemented as it's not used for the Snowflake case.
        """
        raise NotImplementedError("read_many_json")

    def managed_download(self, key: str, download_path: str):
        """
        For this platform, the same as `download_file`.
        """
        self.download_file(key, download_path)

    def list_objects(
        self,
        prefix: Optional[str] = None,
        batch_size: Optional[int] = None,
        continuation_token: Optional[str] = None,
        delimiter: Optional[str] = None,
        *args,  # type: ignore
        **kwargs,  # type: ignore
    ) -> Tuple[Union[List, None], Union[str, None]]:
        """
        Not implemented as it's not used for the Snowflake case.
        """
        raise NotImplementedError("list_objects")

    def generate_presigned_url(self, key: str, expiration: timedelta) -> str:
        """
        This method uses GET_PRESIGNED_URL to generate the pre-signed URL.
        During the implementation phase we found that running this inside the Snowflake App
        generates an invalid URL, but it generates a valid one if we call it from a
        stored procedure.
        So, we're using the `execute_query` stored procedure to wrap the query that
        generates the URL.
        """
        full_key = self._apply_prefix(key) or key
        if full_key.startswith("/"):
            full_key = full_key[1:]
        pre_signed_url_query = (
            f"CALL GET_PRESIGNED_URL("
            f"@{self._stage_name}, '{full_key}', {expiration.total_seconds()})"
        )

        if self._local:
            data, _ = self._run_stage_query(pre_signed_url_query, "pre_signed_url", key)
        else:
            # for some reason, when running in the SNA, we need to request the pre-signed url
            # using a store procedure, the url we get directly using GET_PRESIGNED_URL doesn't work.
            data, _ = self._run_stage_query(
                "CALL mcd_agent.core.execute_query(?)",
                "pre_signed_url",
                key,
                [pre_signed_url_query],
            )

        if data:
            first_row = data[0]
            if first_row:
                return first_row[0]
        raise BaseStorageClient.GenericError("No pre-signed URL returned")

    def is_bucket_private(self) -> bool:
        """
        This is called as part of the storage validation from MC, to confirm the storage
        is properly configured to reject public access, but that's not relevant for
        Snowflake, as internal stages are always private, so we're just returning True.
        """
        return True

    @staticmethod
    def _parse_key(key: str) -> Tuple[str, str]:
        """
        Returns the path to the folder and the file name, this is required because
        some stage operations require the folder and the file name to be passed separately,
        for example when uploading files.
        """
        folder, file_name = os.path.split(key)
        folder = folder + "/" if folder and folder != "/" else ""
        return folder, file_name

    @contextlib.contextmanager
    def _temp_location(
        self, file_name: str, contents: Optional[bytes] = None
    ) -> Iterator[str]:
        """
        Creates a temporary file with the specified contents (if any) and returns the path to it.
        The file is deleted when the context manager exits.
        """
        tmp_location = os.path.join(tempfile.gettempdir(), str(uuid4()), file_name)
        dir_name = os.path.dirname(tmp_location)
        os.makedirs(dir_name, exist_ok=True)

        try:
            if contents:
                with open(tmp_location, "wb") as f:
                    f.write(contents)
            yield tmp_location
        finally:
            if os.path.exists(tmp_location):
                os.remove(tmp_location)
            os.rmdir(dir_name)

    @contextlib.contextmanager
    def _temp_directory(self) -> Iterator[str]:
        """
        Creates a temporary directory and returns the path to it.
        The directory is deleted when the context manager exits.
        """
        tmp_location = os.path.join(tempfile.gettempdir(), str(uuid4()))
        os.makedirs(tmp_location, exist_ok=True)

        try:
            yield tmp_location
        finally:
            os.rmdir(tmp_location)

    def _run_stage_query(
        self,
        query: str,
        operation: str,
        key: str,
        *args,  # type: ignore
    ) -> Tuple[List[Tuple], List[Tuple]]:
        try:
            return self._queries_service.run_query_and_fetch_all(query, *args)
        except OperationalError as err:
            if err.errno == _SNOWFLAKE_ERROR_FILE_NOT_FOUND:
                raise BaseStorageClient.NotFoundError(f"File not found: {key}")
            else:
                raise BaseStorageClient.GenericError(
                    f"{operation} operation failed for: {key}: {err}"
                )
