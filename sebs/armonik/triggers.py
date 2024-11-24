import concurrent.futures
import datetime
import json
import subprocess
from typing import Dict, List, Optional  # noqa

from armonik.client import ArmoniKResults, ArmoniKSessions, ArmoniKTasks
from armonik.common import Result, Task, TaskDefinition, TaskOptions
from armonik.common.channel import create_channel

from sebs.faas.function import ExecutionResult, Trigger


class LibraryTrigger(Trigger):
    def __init__(self, fname: str, control_plane_url):
        super().__init__()
        self.fname = fname
        self._channel = create_channel(control_plane_url).__enter__()
        self._session_client = ArmoniKSessions(self._channel)
        self._task_client = ArmoniKTasks(self._channel)
        self._result_client = ArmoniKResults(self._channel)

        self._session_id = self._session_client.create_session(
            default_task_options=TaskOptions(
                max_duration=datetime.timedelta(minutes=5),
                priority=1,
                max_retries=1,
                partition_id=fname
            ),
            partition_ids=[fname]
        )

    @staticmethod
    def trigger_type() -> "Trigger.TriggerType":
        return Trigger.TriggerType.LIBRARY

    def sync_invoke(self, payload: dict) -> ExecutionResult:
        error = None
        try:
            begin = datetime.datetime.now()
            task = self._task_client.submit_tasks(
                session_id=self._session_id,
                tasks=[TaskDefinition(
                    payload_id="",
                    data_dependencies=[],
                    expected_output_ids=[],
                )],
            )[0]
            
            response = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            end = datetime.datetime.now()
            parsed_response = response.stdout.decode("utf-8")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            end = datetime.datetime.now()
            error = e

        openwhisk_result = ExecutionResult.from_times(begin, end)
        if error is not None:
            self.logging.error("Invocation of {} failed!".format(self.fname))
            openwhisk_result.stats.failure = True
            return openwhisk_result

        return_content = json.loads(parsed_response)
        openwhisk_result.parse_benchmark_output(return_content)
        return openwhisk_result

    def async_invoke(self, payload: dict) -> concurrent.futures.Future:
        pool = concurrent.futures.ThreadPoolExecutor()
        fut = pool.submit(self.sync_invoke, payload)
        return fut

    def serialize(self) -> dict:
        return {"type": "Library", "name": self.fname}

    @staticmethod
    def deserialize(obj: dict) -> Trigger:
        return LibraryTrigger(obj["name"])

    @staticmethod
    def typename() -> str:
        return "ArmoniK.LibraryTrigger"
