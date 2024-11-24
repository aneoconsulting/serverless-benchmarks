import os
import uuid
from typing import List, Optional

from armonik.client import ArmoniKResults
from armonik.common import Result, Direction

from sebs.cache import Cache
from sebs.faas.config import Resources
from ..faas.storage import PersistentStorage


class ArmoniKStorage(PersistentStorage):
    @staticmethod
    def typename() -> str:
        return "ArmoniK.ArmoniKStorage"

    @staticmethod
    def deployment_name():
        return "armonik"

    def __init__(
        self, cache_client: Cache, resources: Resources, replace_existing: bool, result_client: ArmoniKResults, session_id: str
    ) -> None:
        super().__init__(region="", cache_client=cache_client, resources=resources, replace_existing=replace_existing)
        self._result_client = result_client
        self._session_id = session_id

    def correct_name(self, name: str) -> str:
        return name

    def _create_bucket(
        self, name: str, buckets: List[str] = [], randomize_name: bool = False
    ) -> str:
        pass

    def download(self, bucket_name: str, key: str, filepath: str) -> None:
        """Download a file from a bucket.

        Args:
            bucket_name:
            key: storage source filepath
            filepath: local destination filepath
        """
        total, results = self._result_client.list_results(
            result_filter=(Result.name == f"{bucket_name}/{key}"),
            page=0,
            page_size=10,
            sort_field=Result.completed_at,
            sort_direction=Direction.DESC,
        )
        if total > 1:
            self.logging.warning(f"Found {total} versions of the object {key} for bucket {bucket_name}. Use the most recently completed one.")
        with open(filepath, "wb") as file:
            file.write(self._result_client.download_result_data(
                result_id=results[0].result_id,
                session_id=self._session_id,
            ))

    def upload(self, bucket_name: str, filepath: str, key: str):
        """Upload a file to a bucket with by passing caching.
        Useful for uploading code package to storage (when required).

        Args:
            bucket_name:
            filepath: local source filepath
            key: storage destination filepath
        """
        result = self._result_client.create_results_metadata(result_names=[f"{bucket_name}/{key}"], session_id=self._session_id).values()[0]
        with open(filepath, "rb") as file:
            self._result_client.upload_result_data(
                result_data=file.read(),
                result_id=result.result_id,
                session_id=self._session_id,
            )

    def list_bucket(self, bucket_name: str, prefix: str = "") -> List[str]:
        """Retrieves list of files in a bucket.

        Args:
            bucket_name: name of the bucket.
        
        Return:
            list of files in a given bucket
        """
        objects = set()
        page = 0
        list_args = {
            "result_filter": (f"{bucket_name}/{prefix}" in Result.name),
            "page": page,
            "sort_field": Result.result_id,
            "sort_direction": Direction.ASC,
        }
        total, results = self._result_client.list_results(**list_args)
        if total == 0:
            return []
        while results:
            objects.union([r.name for r in results])
            page += 1
            _, results = self._result_client.list_results(**list_args)
        return objects

    def list_buckets(self, bucket_name: Optional[str] = None) -> List[str]:
        buckets = set()
        page = 0
        total, results = self._result_client.list_results(page=page)
        if total == 0:
            return []
        while results:
            for result in results:
                buckets.add(result.name.split("/")[0])
            page += 1
            _, resulsts = self._result_client.list_results(page=page)
        if bucket_name is not None:
            return [bucket for bucket in buckets if bucket_name in bucket]
        return list(buckets)

    def exists_bucket(self, bucket_name: str) -> bool:
        total, _ = self._result_client.list_results(
            result_filter=(bucket_name in Result.name),
            page=0,
            page_size=1,
            sort_field=Result.result_id,
            sort_direction=Direction.ASC,
        )
        if total > 0:
            return True
        return False

    def clean_bucket(self, bucket_name: str):
        pass

    def remove_bucket(self, bucket: str):
        pass

    def uploader_func(self, bucket_idx: int, file: str, filepath: str) -> None:
        """Implements a handy routine for uploading input data by benchmarks.
        It should skip uploading existing files unless storage client has been
        initialized to override existing data.

        Args:
            bucket_idx: index of input bucket
            file: name of file to upload
            filepath: filepath in the storage
        """
        pass
