import os
import uuid
from typing import List, Optional

from armonik.client import ArmoniKResults
from armonik.common import Result, Direction, ResultStatus

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

    @staticmethod
    def _get_name_or_prefix(bucket: str, key: Optional[str] = None, prefix: Optional[str] = None) -> str:
        name = f"bucket={bucket},key="
        if key:
            return name + key
        if prefix:
            return name + prefix
        return name

    @staticmethod
    def _get_bucket_from_name(result_name: str) -> str:
        return result_name.split(",")[0].removeprefix("bucket=")

    def _create_bucket(
        self, name: str, buckets: List[str] = [], randomize_name: bool = False
    ) -> str:
        for bucket_name in buckets:
            if name in bucket_name:
                self.logging.info(
                    "Bucket {} for {} already exists, skipping.".format(bucket_name, name)
                )
                return bucket_name

        if randomize_name:
            random_name = str(uuid.uuid4())[0:16]
            bucket_name = "{}-{}".format(name, random_name)
        else:
            bucket_name = name

        self.logging.info("Created bucket {}".format(bucket_name))
        return bucket_name


    def download(self, bucket_name: str, key: str, filepath: str) -> None:
        """Download a file from a bucket.

        Args:
            bucket_name:
            key: storage source filepath
            filepath: local destination filepath
        """
        total, results = self._result_client.list_results(
            result_filter=(Result.name == self._get_name_or_prefix(bucket_name, key) + (Result.status == ResultStatus.COMPLETED)),
            page=0,
            page_size=5,
            sort_field=Result.completed_at,
            sort_direction=Direction.DESC,
        )
        if total == 0:
            raise ValueError(f"Object {key} from bucket {bucket_name} not found. No corresponding result.")
        if total > 1:
            self.logging.warning(f"Found {total} versions of the object {key} for bucket {bucket_name}. Use the most recently completed one.")
        self.logging.info(f"Object {key} from bucket {bucket_name} corresponds to result with id {results[0].result_id}.")
        with open(filepath, "wb") as file:
            file.write(self._result_client.download_result_data(
                result_id=results[0].result_id,
                session_id=self._session_id,
            ))
            self.logging.info(f"Downloaded object {key} from bucket {bucket_name}.")

    def upload(self, bucket_name: str, filepath: str, key: str):
        """Upload a file to a bucket with by passing caching.
        Useful for uploading code package to storage (when required).

        Args:
            bucket_name:
            filepath: local source filepath
            key: storage destination filepath
        """
        result_name = self._get_name_or_prefix(bucket_name, key)
        result = self._result_client.create_results_metadata([result_name], self._session_id).values()[result_name]
        self.logging.info(f"Created result {result.result_id} for object {key} in bucket {bucket_name}.")
        with open(filepath, "rb") as file:
            self._result_client.upload_result_data(
                result_data=file.read(),
                result_id=result.result_id,
                session_id=self._session_id,
            )
            self.logging.info(f"Uploaded object {key} to bucket {bucket_name}.")

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
            "result_filter": Result.name.startswith(self._get_name_or_prefix(bucket_name, prefix)) and (Result.status == ResultStatus.COMPLETED),
            "page": page,
            "sort_field": Result.result_id,
            "sort_direction": Direction.ASC,
        }
        total, results = self._result_client.list_results(**list_args)
        if total == 0:
            self.logging.warning(f"No object found in bucket {bucket_name} for prefix {prefix}.")
            return []
        while results:
            objects.union([r.name for r in results])
            page += 1
            _, results = self._result_client.list_results(**list_args)
        self.logging.info(f"Found {len(objects)} in bucket {bucket_name} for prefix {prefix}.")
        return objects

    def list_buckets(self, bucket_name: Optional[str] = None) -> List[str]:
        buckets = set()
        page = 0
        total, results = self._result_client.list_results(result_filter=Result.status == ResultStatus.COMPLETED, page=page)
        if total == 0:
            self.logging.warning("No bucket exists.")
            return []
        while results:
            for result in results:
                buckets.add(self._get_bucket_from_name(result.name))
            page += 1
            _, results = self._result_client.list_results(page=page)
        if bucket_name is not None:
            buckets = [bucket for bucket in buckets if bucket_name in bucket]
        else:
            buckets = list(buckets)
        self.logging.info(f"Found {len(buckets)} buckets.")
        return buckets

    def exists_bucket(self, bucket_name: str) -> bool:
        total, _ = self._result_client.list_results(
            result_filter=Result.name.startswith(self._get_name_or_prefix(bucket_name)) + (Result.status == ResultStatus.COMPLETED),
            page=0,
            page_size=1,
            sort_field=Result.result_id,
            sort_direction=Direction.ASC,
        )
        if total > 0:
            self.logging.info(f"Bucket {bucket_name} exists.")
            return True
        self.logging.info(f"Bucket {bucket_name} doesn't exist.")
        return False

    def clean_bucket(self, bucket_name: str):
        n_deletion = 0
        page = 0
        page_size = 100
        list_args = {
            "result_filter": Result.name.startswith(self._get_name_or_prefix(bucket_name)) + (Result.status == ResultStatus.COMPLETED),
            "page": page,
            "page_size": page_size,
            "sort_field": Result.result_id,
            "sort_direction": Direction.ASC,
        }
        total, results = self._result_client.list_results(**list_args)
        if total == 0:
            self.logging.warning(f"No object found in bucket {bucket_name}.")
            return []
        while results:
            self._result_client.delete_result_data(
                result_ids=[r.result_id for r in results],
                session_id=self._session_id,
                batch_size=page_size,
            )
            n_deletion += len(results)
            page += 1
            _, results = self._result_client.list_results(**list_args)
        self.logging.info(f"Delete {n_deletion} objects from bucket {bucket_name}.")

    def remove_bucket(self, bucket: str):
        self.clean_bucket(bucket)
        self.logging.info(f"Removed bucket {bucket}.")

    def uploader_func(self, bucket_idx: int, file: str, filepath: str) -> None:
        """Implements a handy routine for uploading input data by benchmarks.
        It should skip uploading existing files unless storage client has been
        initialized to override existing data.

        Args:
            bucket_idx: index of input bucket
            file: name of file to upload
            filepath: filepath in the storage
        """
        raise NotImplementedError("ArmoniK platform only support function deployment through container image.")
