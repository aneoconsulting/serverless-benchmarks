import os
import shutil

from threading import Lock

class SingletonMeta(type):
    """
    A thread-safe implementation of the Singleton pattern.

    Attributes:
        _instance: The single instance of the class. Initially set to None.
        _lock: A threading lock to ensure thread-safe access to the Singleton instance.
    """

    _instance: object = None.
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs) -> object:
        """
        Ensures only one instance of the class is created, even in a multi-threaded environment.
        
        Args:
            *args: Positional arguments for the class constructor.
            **kwargs: Keyword arguments for the class constructor.
        
        Returns:
            The single instance of the class.
        """
        # Use a lock to ensure thread-safe instance creation.
        with cls._lock:
            if cls._instance is None:
                # Create the Singleton instance if it doesn't exist.
                cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class storage(metaclass=SingletonMeta):

    def __init__(self, blob_ids: dict[tuple[str, str], str] | None = None, blob_locs: dict[str, str] | None = None) -> None:
        self.blob_ids = blob_ids
        self.blob_locs = blob_locs

    def upload(self, bucket: str, file: str, filepath: str) -> str:
        blob_id = self.blob_ids[bucket, file]
        shutil.copy(filepath, self.blob_locs(blob_id))
        return blob_id

    def download(self, bucket: str, file: str, filepath: str) -> None:
        blob_id = self.blob_ids[bucket, file]
        shutil.copy(self.blob_locs(blob_id), filepath)

    def download_directory(self, bucket: str, prefix: str, path: str) -> None:
        files = [self.blob_locs[blob_id].split("/")[-1] for (buck, file), blob_id in self.blob_ids.values() if buck == bucket and file.startswith(prefix)]
        for file in files:
            os.makedirs(os.path.join(path, os.path.dirname(file)), exist_ok=True)
            self.download(bucket, file, os.path.join(path, file))

    def upload_stream(self, bucket: str, file: str, data: bytes) -> str:
        blob_id = self.blob_ids[bucket, file]
        with open(self.blob_locs[blob_id], "wb") as f:
            f.write(data)
        return blob_id

    def download_stream(self, bucket: str, file: str) -> bytes:
        blob_id = self.blob_ids[bucket, file]
        with open(self.blob_locs[blob_id], "rb") as f:
            data = f.read()
        return data

    @staticmethod
    def get_instance() -> "storage":
        if storage._instance is None:
            raise RuntimeError("Storage class not initialized.")
        return storage()
