import json
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional, Union, Dict, Tuple, List, cast


class BaseStorageClient(ABC):
    """
    The base class for storage clients with operations to list, read, write files and generate signed urls.
    """

    _GZIP_MAGIC_NUMBER = (
        b"\x1f\x8b"  # Hex signature used to identify a gzip compressed files
    )

    class GenericError(Exception):
        pass

    class PermissionsError(GenericError):
        pass

    class NotFoundError(GenericError):
        pass

    def __init__(self, prefix: Optional[str] = None):
        self._prefix = prefix or ""
        if self._prefix and not self._prefix.endswith("/"):
            self._prefix = f"{self._prefix}/"

    @property
    @abstractmethod
    def bucket_name(self) -> str:
        """
        Returns the bucket name referenced by this client
        """
        pass

    @abstractmethod
    def write(self, key: str, obj_to_write: Union[bytes, str]) -> None:
        """
        Writes a file in the given key, contents are included as bytes or string.
        :param key: path to the file, for example /dir/name.ext
        :param obj_to_write: contents for the file, specified as a bytes array or string
        """
        raise NotImplementedError()

    @abstractmethod
    def read(
        self,
        key: str,
        decompress: Optional[bool] = False,
        encoding: Optional[str] = None,
    ) -> Union[bytes, str]:
        """
        Returns the contents of the specified file.
        :param key: path to the file, for example /dir/name.ext
        :param decompress: flag indicating if `gzip` contents should be decompressed automatically
        :param encoding: if set binary content will be decoded using this encoding and a string will be returned
        :return: a bytes object, unless encoding is set, in which case it returns a string.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, key: str) -> None:
        """
        Deletes the file at `key`
        :param key: path to the file, for example /dir/name.ext
        """
        raise NotImplementedError()

    @abstractmethod
    def download_file(self, key: str, download_path: str) -> None:
        """
        Downloads the file at `key` to the local file indicated by `download_path`.
        :param key: path to the file, for example /dir/name.ext
        :param download_path: local path to the file where the contents of `key` will be stored.
        """
        raise NotImplementedError()

    @abstractmethod
    def upload_file(self, key: str, local_file_path: str) -> None:
        """
        Uploads the file at `local_file_path` to `key` in the associated bucket.
        :param key: path to the file, for example /dir/name.ext
        :param local_file_path: local path to the file to upload.
        """
        raise NotImplementedError()

    def read_json(self, key: str) -> Dict:
        """
        Returns the contents as a dictionary of the JSON file at `key`.
        :param key: path to the file, for example /dir/name.ext
        :return: a Dictionary loaded from the JSON document.
        """
        data = self.read(key)
        return json.loads(data.decode("utf-8") if isinstance(data, bytes) else data)

    @abstractmethod
    def read_many_json(self, prefix: str) -> Dict:
        """
        Reads all JSON files under `prefix` and returns a dictionary where the key is the file path and the value
        is the dictionary loaded from the JSON file.
        :param prefix: Prefix for the files to load, for example: `/dir/`
        :return: a dictionary where the key is the file path and the value is the dictionary loaded from the JSON file.
        """
        raise NotImplementedError()

    @abstractmethod
    def managed_download(self, key: str, download_path: str):
        """
        Performs a managed transfer that might be multipart, downloads the file at `key` to the local file at
        `download_path`.
        :param key: path to the file, for example /dir/name.ext
        :param download_path: local path to the file where the contents of `key` will be stored.
        """
        raise NotImplementedError()

    @abstractmethod
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
        List objects (files and folder) under the specified prefix.
        Delimiter is set to "/" to return sub-folders, this works for all storage providers as it works for S3.
        Documentation about delimiter in S3 requests available here:
        https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_RequestSyntax
        Prefix can be used to return contents of folders.
        :param prefix: Prefix to use for listing, it can be used to list folders, for example: `prefix=/dir/`
        :param batch_size: Used to page the result
        :param continuation_token: Used to page the result, the second value in the resulting tuple is the continuation
            token for the next call.
        :param delimiter: Set to "/" to return sub-folders, when set the result will include the list of prefixes
            returned by the storage provider instead of metadata for the objects.
        :return: A tuple with the result list and the continuation token. The result list includes the following
            attributes (when no delimiter is set): ETag, Key, Size, LastModified, StorageClass. If delimiter is
            specified only Prefix is included in the result for each listed folder.
        """
        raise NotImplementedError()

    @abstractmethod
    def generate_presigned_url(self, key: str, expiration: timedelta) -> str:
        """
        Generates a pre-signed url for the given file with the specified expiration.
        :param key: path to the file, for example /dir/name.ext
        :param expiration: time for the generated link to expire, expressed as a timedelta object.
        :return: a pre-signed url to access the specified file.
        """
        raise NotImplementedError()

    @abstractmethod
    def is_bucket_private(self) -> bool:
        """
        Checks if the bucket is configured with public access disabled.

        :return: True if public access is disabled for the bucket and False if the bucket is publicly available.
        """
        raise NotImplementedError()

    def check_storage_access(self):
        """
        Checks if we have access to the storage account, called from the DC as part of the
        storage access validation.
        """
        pass

    def _is_gzip(self, content: bytes) -> bool:
        return content[:2] == self._GZIP_MAGIC_NUMBER

    def _apply_prefix(self, key: Optional[str]) -> Optional[str]:
        if not key:
            return self._prefix
        if self._prefix:
            return f"{self._prefix}{key}"
        else:
            return key

    def _remove_prefix(self, key: str) -> str:
        if self._prefix and key.startswith(self._prefix):
            return key[len(self._prefix) :]
        else:
            return key

    def _remove_prefix_from_prefixes(
        self, entries: Optional[List[Dict]]
    ) -> Optional[List[Dict]]:
        if not entries or not self._prefix:
            return entries
        return [
            {"Prefix": self._remove_prefix(cast(str, entry.get("Prefix")))}
            for entry in entries
        ]

    def _remove_prefix_from_entries(
        self, entries: Optional[List[Dict]]
    ) -> Optional[List[Dict]]:
        if not entries or not self._prefix:
            return entries
        return [
            {**entry, "Key": self._remove_prefix(cast(str, entry.get("Key")))}
            for entry in entries
        ]
