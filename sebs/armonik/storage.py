import os
import uuid
from typing import List, Optional

from armonik.client import ArmoniKResults
from armonik.common import Result, Direction, ResultStatus
from grpc import RpcError

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
        self.result_ids = {}

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

        self.result_ids[bucket_name] = {}
        self.logging.info("Created bucket {}".format(bucket_name))
        return bucket_name

    def download(self, bucket_name: str, key: str, filepath: str) -> None:
        """Download a file from a bucket.

        Args:
            bucket_name:
            key: storage source filepath
            filepath: local destination filepath
        """
        if bucket_name not in self.result_ids:
            raise RuntimeError(f"Attempt to download an object from a non-existing bucket: {bucket_name}.")
        if key not in self.result_ids[bucket_name]:
            raise RuntimeError(f"Object {key} not found in bucket {bucket_name}.")
        result_id = self.result_ids[bucket_name][key]
        if self._result_client.get_result(result_id=result_id).status != ResultStatus.COMPLETED:
            raise RuntimeError(f"Object {key} in bucket {bucket_name} corresponds to result {result_id} that is empty.")
        with open(filepath, "wb") as file:
            file.write(self._result_client.download_result_data(
                result_id=result_id,
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
        if bucket_name not in self.result_ids:
            raise ValueError(f"Attempt to upload a file into an non-existing bucket: {bucket_name}.")
        result_name = self._get_name_or_prefix(bucket_name, key)
        result_id = self._result_client.create_results_metadata([result_name], self._session_id)[result_name].result_id
        self.result_ids[bucket_name][key] = result_id
        self.logging.info(f"Created result {result_id} for object {key} in bucket {bucket_name}.")
        with open(filepath, "rb") as file:
            self._result_client.upload_result_data(
                result_data=file.read(),
                result_id=result_id,
                session_id=self._session_id,
            )
            self.logging.info(f"Uploaded object {key} to bucket {bucket_name}.")

    def create_empty_object(self, bucket_name: str, key: str):
        """Create an empty result corresponding to an object to be created by a function invocation.
        Required for ArmoniK to invoke the function.

        Args:
            bucket_name:
            key: storage destination filepath
        """
        if bucket_name not in self.result_ids:
            raise ValueError(f"Attempt to upload a file into an non-existing bucket: {bucket_name}.")
        result_name = self._get_name_or_prefix(bucket_name, key)
        result_id = self._result_client.create_results_metadata([result_name], self._session_id)[result_name].result_id
        self.result_ids[bucket_name][key] = result_id
        self.logging.info(f"Created result {result_id} for object {key} in bucket {bucket_name}.")

    def list_bucket(self, bucket_name: str, prefix: str = "") -> List[str]:
        """Retrieves list of files in a bucket.

        Args:
            bucket_name: name of the bucket.
        
        Return:
            list of files in a given bucket
        """
        if bucket_name in self.result_ids:
            objects = [*self.result_ids[bucket_name].keys()]
        else:
            objects = []
        self.logging.info(f"Found {len(objects)} in bucket {bucket_name} for prefix {prefix}.")
        return objects

    def list_buckets(self, bucket_name: Optional[str] = None) -> List[str]:
        buckets = [*self.result_ids.keys()]
        if bucket_name is not None:
            buckets = [bucket for bucket in buckets if bucket_name in bucket]
        self.logging.info(f"Found {len(buckets)} buckets.")
        return buckets

    def exists_bucket(self, bucket_name: str) -> bool:
        if bucket_name in self.result_ids:
            self.logging.info(f"Bucket {bucket_name} exists.")
            return True
        self.logging.info(f"Bucket {bucket_name} doesn't exist.")
        return False

    def clean_bucket(self, bucket_name: str):
        if bucket_name in self.result_ids:
            n_deletion = len(self.result_ids[bucket_name])
            if n_deletion > 0:
                self._result_client.delete_result_data(
                    result_ids=[*self.result_ids[bucket_name].values()],
                    session_id=self._session_id,
                    batch_size=100,
                )
                self.result_ids[bucket_name] = {}
                self.logging.info(f"Delete {n_deletion} objects from bucket {bucket_name}.")
            else:
                self.logging.warning(f"Attempt to clean an empty bucket: {bucket_name}.")
        else:
            self.logging.warning(f"Attempt to clean a non-existent bucket: {bucket_name}.")

    def remove_bucket(self, bucket: str):
        if bucket in self.result_ids:
            self.clean_bucket(bucket)
            del self.result_ids[bucket]
            self.logging.info(f"Removed bucket {bucket}.")
        else:
            self.logging.warning(f"Attempt to delete a non-existent bucket: {bucket}.")

    def uploader_func(self, path_idx: int, key: str, filepath: str) -> None:
        """Implements a handy routine for uploading input data by benchmarks.
        It should skip uploading existing files unless storage client has been
        initialized to override existing data.

        Args:
            bucket_idx: index of input bucket
            file: name of file to upload
            filepath: filepath in the storage
        """
        key = os.path.join(self.input_prefixes[path_idx], key)
        bucket_name = self.get_bucket(Resources.StorageBucketType.BENCHMARKS)
        self.upload(bucket_name, filepath, key)
