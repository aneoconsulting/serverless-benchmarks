import concurrent.futures
import datetime
import json
from typing import List, Optional  # noqa

from armonik.client import ArmoniKResults, ArmoniKEvents, ArmoniKTasks
from armonik.common import TaskDefinition, TaskStatus

from sebs.faas.function import ExecutionResult, Trigger


class LibraryTrigger(Trigger):
    def __init__(self, fname: str, deployment_client: Optional[ArmoniK] = None):
        super().__init__()
        self.name = fname
        self._deployment_client = deployment_client

    @staticmethod
    def typename() -> str:
        return "ArmoniK.LibraryTrigger"

    @property
    def deployment_client(self) -> AWS:
        assert self._deployment_client
        return self._deployment_client

    @deployment_client.setter
    def deployment_client(self, deployment_client: AWS):
        self._deployment_client = deployment_client

    @staticmethod
    def trigger_type() -> "Trigger.TriggerType":
        return Trigger.TriggerType.LIBRARY

    def sync_invoke(self, payload: dict) -> ExecutionResult:
        self.logging.info(f"Invoke function {self.name}")

        task_client: ArmoniKTasks = self.deployment_client.get_task_client()
        result_client: ArmoniKResults = self.deployment_client.get_result_client()
        event_client: ArmoniKEvents = self.deployment_client.get_event_client()
        session_id = self.deployment_client.get_session_id()

        request_body, task_payload = result_client.create_results(results_data={"request_body": json.dumps(payload).encode("utf-8"), "task_payload": {}}, session_id=session_id).values()
        response_body = result_client.create_results_metadata(result_names=["response_body"], session_id=session_id)
        begin = datetime.datetime.now()
        func_task = task_client.submit_tasks(
            session_id=session_id,
            tasks=[TaskDefinition(
                payload_id=task_payload.result_id,
                data_dependencies=[],
                expected_output_ids=[],
            )],
        )[0]
        event_client.wait_for_result_availability(result_ids=[response_id], session_id=session_id)
        ret = json.loads(result_client.download_result_data(result_id=response_id, session_id=session_id).decode("utf-8"))
        end = datetime.datetime.now()

        func_task.refresh(task_client=task_client)

        armonik_result = ExecutionResult.from_times(begin, end)
        armonik_result.request_id = ret["RequestId"]
        if func_task.status != TaskStatus.COMPLETED:
            self.logging.error("Invocation of {} failed! Status {}.".format(self.name, func_task.status.name.lower()))
            self.logging.error("Input: {}".format(payload))
            armonik_result.stats.failure = True
            return armonik_result

        self.logging.debug(f"Invoke of function {self.name} was successful")
        armonik_result.parse_benchmark_output(ret)

        return armonik_result

    def async_invoke(self, payload: dict) -> concurrent.futures.Future:
        pool = concurrent.futures.ThreadPoolExecutor()
        fut = pool.submit(self.sync_invoke, payload)
        return fut

    def serialize(self) -> dict:
        return {"type": "Library", "name": self.name}

    @staticmethod
    def deserialize(obj: dict) -> Trigger:
        return LibraryTrigger(obj["name"])
