import concurrent.futures
import datetime
import json
from typing import List, Optional  # noqa

from armonik.client import ArmoniKResults, ArmoniKEvents, ArmoniKTasks
from armonik.common import TaskDefinition, TaskStatus, TaskOptions

from sebs.faas.function import ExecutionResult, Trigger
from sebs.armonik.armonik import ArmoniK


class LibraryTrigger(Trigger):
    def __init__(self, fname: str, deployment_client: Optional[ArmoniK] = None):
        super().__init__()
        self.name = fname
        self._deployment_client = deployment_client

    @staticmethod
    def typename() -> str:
        return "ArmoniK.LibraryTrigger"

    @property
    def deployment_client(self) -> ArmoniK:
        assert self._deployment_client
        return self._deployment_client

    @deployment_client.setter
    def deployment_client(self, deployment_client: ArmoniK):
        self._deployment_client = deployment_client

    @staticmethod
    def trigger_type() -> "Trigger.TriggerType":
        return Trigger.TriggerType.LIBRARY

    def sync_invoke(self, payload: dict) -> ExecutionResult:
        self.logging.info(f"Invoke function {self.name}.")

        task_client: ArmoniKTasks = self.deployment_client.get_task_client()
        result_client: ArmoniKResults = self.deployment_client.get_result_client()
        event_client: ArmoniKEvents = self.deployment_client.get_event_client()
        session_id = self.deployment_client.get_session_id()
        storage_client = self.deployment_client.storage

        self.logging.info(f"Function invoked within session {session_id}.")
        self.logging.info(f"Using partition {self.deployment_client.get_function_partition(self.name)}.")
        task_options = TaskOptions(partition_id=self.deployment_client.get_function_partition(self.name), max_retries=1, max_duration=datetime.timedelta(minutes=10), priority=1)

        # TODO:
        # - create separate function to extract input/output from payload if there is some
        # - add input/output to data dependencies and expected output ids
        # - clean-up the code

        input_blobs, output_blobs = self._get_blobs_from_payload(payload)

        request_body = result_client.create_results(results_data={"request_body": json.dumps(payload).encode("utf-8")}, session_id=session_id)["request_body"]
        response_body = result_client.create_results_metadata(result_names=["response_body"], session_id=session_id)["response_body"]
        task_payload = result_client.create_results(
            results_data={
                "payload": json.dumps({
                    "request_body": request_body.result_id,
                    "response_body": response_body.result_id,
                    "blobs": {
                        f"{bucket},{blob}": storage_client.result_ids[bucket][blob]
                        for bucket, blob in input_blobs + output_blobs
                    }
                }).encode("utf-8")},
            session_id=session_id)["payload"]
        begin = datetime.datetime.now()
        func_task = task_client.submit_tasks(
            session_id=session_id,
            tasks=[TaskDefinition(
                payload_id=task_payload.result_id,
                data_dependencies=[request_body.result_id] + [storage_client.result_ids[bucket][blob] for bucket, blob in input_blobs] if input_blobs else [],
                expected_output_ids=[response_body.result_id] + [storage_client.result_ids[bucket][blob] for bucket, blob in output_blobs] if output_blobs else [],
                options=task_options,
            )],
        )[0]
        self.logging.info(f"Invocation request ID: {func_task.id}.")
        event_client.wait_for_result_availability(result_ids=[response_body.result_id], session_id=session_id)
        ret = json.loads(result_client.download_result_data(result_id=response_body.result_id, session_id=session_id).decode("utf-8"))
        end = datetime.datetime.now()

        func_task.refresh(task_client=task_client)

        armonik_result = ExecutionResult.from_times(begin, end)
        armonik_result.request_id = ret["request_id"]
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

    @staticmethod
    def _get_blobs_from_payload(storage_client, payload: dict) -> Tuple[List[Tuple[str, str]],List[Tuple[str, str]]]:
        bucket = payload["bucket"]["bucket"]
        input_blob = f"{payload['bucket']['input']}/{payload['object']['key']}"
        output_blob = f"{payload['bucket']['output']}/{payload['object']['key']}"
        storage_client.create_empty_object(bucket, output_blob)
        return [bucket, input_blob], [bucket, output_blob]
