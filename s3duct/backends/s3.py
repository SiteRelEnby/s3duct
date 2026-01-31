"""AWS S3 storage backend."""

import time
from pathlib import Path

import boto3
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
    ReadTimeoutError,
)

from s3duct.backends.base import ObjectInfo, StorageBackend
from s3duct.config import MAX_RETRY_ATTEMPTS, RETRY_BASE_DELAY, RETRY_MAX_DELAY

# Errors worth retrying: API errors + connection-level failures
_RETRYABLE = (ClientError, ConnectionClosedError, ConnectTimeoutError,
              EndpointConnectionError, ReadTimeoutError, ConnectionError, OSError)


class S3Backend(StorageBackend):

    def __init__(self, bucket: str, region: str | None = None, prefix: str = "",
                 endpoint_url: str | None = None,
                 max_retries: int = MAX_RETRY_ATTEMPTS,
                 retry_base_delay: float = RETRY_BASE_DELAY,
                 retry_max_delay: float = RETRY_MAX_DELAY) -> None:
        self._bucket = bucket
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay
        session = boto3.Session(region_name=region)
        self._client = session.client("s3", endpoint_url=endpoint_url)

    def preflight_check(self) -> None:
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except NoCredentialsError:
            raise RuntimeError(
                "No AWS credentials found. Configure credentials via environment "
                "variables, ~/.aws/credentials, IAM role, or SSO."
            )
        except PartialCredentialsError as e:
            raise RuntimeError(f"Incomplete AWS credentials: {e}")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "403":
                raise RuntimeError(
                    f"Access denied to bucket {self._bucket!r}. "
                    "Check your IAM permissions."
                )
            elif code == "404":
                raise RuntimeError(
                    f"Bucket {self._bucket!r} does not exist."
                )
            raise

    def _full_key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def upload(self, key: str, file_path: Path, storage_class: str | None = None) -> str:
        full_key = self._full_key(key)
        extra_args = {}
        if storage_class:
            extra_args["StorageClass"] = storage_class

        for attempt in range(self._max_retries):
            try:
                self._client.upload_file(
                    str(file_path),
                    self._bucket,
                    full_key,
                    ExtraArgs=extra_args or None,
                )
                resp = self._client.head_object(Bucket=self._bucket, Key=full_key)
                return resp["ETag"]
            except _RETRYABLE:
                if attempt == self._max_retries - 1:
                    raise
                delay = min(self._retry_base_delay * (2 ** attempt), self._retry_max_delay)
                time.sleep(delay)
        raise RuntimeError("unreachable")

    def upload_bytes(self, key: str, data: bytes, storage_class: str | None = None) -> str:
        full_key = self._full_key(key)
        kwargs: dict = {"Bucket": self._bucket, "Key": full_key, "Body": data}
        if storage_class:
            kwargs["StorageClass"] = storage_class

        for attempt in range(self._max_retries):
            try:
                resp = self._client.put_object(**kwargs)
                return resp["ETag"]
            except _RETRYABLE:
                if attempt == self._max_retries - 1:
                    raise
                delay = min(self._retry_base_delay * (2 ** attempt), self._retry_max_delay)
                time.sleep(delay)
        raise RuntimeError("unreachable")

    def download(self, key: str, dest_path: Path) -> None:
        full_key = self._full_key(key)
        for attempt in range(self._max_retries):
            try:
                self._client.download_file(self._bucket, full_key, str(dest_path))
                return
            except _RETRYABLE:
                if attempt == self._max_retries - 1:
                    raise
                delay = min(self._retry_base_delay * (2 ** attempt), self._retry_max_delay)
                time.sleep(delay)

    def download_bytes(self, key: str) -> bytes:
        full_key = self._full_key(key)
        for attempt in range(self._max_retries):
            try:
                resp = self._client.get_object(Bucket=self._bucket, Key=full_key)
                return resp["Body"].read()
            except _RETRYABLE:
                if attempt == self._max_retries - 1:
                    raise
                delay = min(self._retry_base_delay * (2 ** attempt), self._retry_max_delay)
                time.sleep(delay)
        raise RuntimeError("unreachable")

    def list_objects(self, prefix: str) -> list[ObjectInfo]:
        full_prefix = self._full_key(prefix)
        objects = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if self._prefix and key.startswith(self._prefix):
                    key = key[len(self._prefix):]
                objects.append(ObjectInfo(
                    key=key,
                    size=obj["Size"],
                    etag=obj["ETag"],
                    storage_class=obj.get("StorageClass"),
                ))
        return objects

    def head_object(self, key: str) -> ObjectInfo:
        full_key = self._full_key(key)
        resp = self._client.head_object(Bucket=self._bucket, Key=full_key)
        return ObjectInfo(
            key=key,
            size=resp["ContentLength"],
            etag=resp["ETag"],
            storage_class=resp.get("StorageClass"),
            restore_status=resp.get("Restore"),
        )

    def delete_object(self, key: str) -> None:
        full_key = self._full_key(key)
        self._client.delete_object(Bucket=self._bucket, Key=full_key)

    def initiate_restore(self, key: str, days: int, tier: str) -> None:
        full_key = self._full_key(key)
        self._client.restore_object(
            Bucket=self._bucket,
            Key=full_key,
            RestoreRequest={"Days": days, "GlacierJobParameters": {"Tier": tier}},
        )

    def is_restore_complete(self, key: str) -> bool:
        info = self.head_object(key)
        if info.restore_status is None:
            return False
        return 'ongoing-request="false"' in info.restore_status
