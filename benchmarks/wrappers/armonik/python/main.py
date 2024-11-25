import json
import logging
import os

from datetime import datetime

from armonik.common import Output
from armonik.protogen.common.agent_common_pb2 import NotifyResultDataRequest
from armonik.worker import TaskHandler, ClefLogger
from armonik.worker.worker import armonik_worker

from . import handler, storage


ClefLogger.setup_logging(logging.INFO)
logger = ClefLogger.getLogger("ArmoniKWorker")


@armonik_worker(logger=logger)
def processor(task_handler: TaskHandler) -> Output:
    logger.info(f"Handeling request of ID {task_handler.task_id}.")
    income_timestamp = datetime.now().timestamp()

    # Deserialize task payload
    name_id_mapping = json.loads(task_handler.payload.decode("utf-8"))
    logger.debug(f"Deserialized payload: {name_id_mapping}")

    # Retrieve function request body
    req_json = json.loads(task_handler.data_dependencies[name_id_mapping["request_body"]].decode("utf-8"))
    logger.debug(f"Request body: {req_json}.")

    # Instantiates the storage interface
    blob_ids = {tuple(k.split("/")): v for k, v in name_id_mapping.items() if k not in ["request_body", "response_body"]}
    blob_locs = {blob_id: os.path.join(task_handler.data_folder, blob_id) for blob_id in blob_ids.keys()}
    storage.storage(blob_ids=blob_ids, blob_locs=blob_locs)

    error_msg = ""
    try:
        ret = handler.request_handler(task_handler.task_id, req_json, income_timestamp)
        task_handler._client.NotifyResultData(request=NotifyResultDataRequest(
            ids=[
                    NotifyResultDataRequest.ResultIdentifier(
                    session_id=task_handler.session_id, result_id=blod_id
                )
                for blod_id in set(task_handler.expected_results) - {name_id_mapping["response_body"]}
            ],
            communication_token=task_handler.token,
        ))
    except Exception as error:
        error_msg = f"Error - Invocation failed! Reason: {error}."
        logger.error(error_msg)
        ret = {
            "begin": "",
            "end": "",
            "request_id": task_handler.task_id,
            "results_time": "",
            "result": error_msg
        }
    finally:
        task_handler.send_results({name_id_mapping["response_body"]: json.dumps(ret).encode("utf-8")})

        if error_msg:
            return Output(error=error_msg)
        return Output()


if __name__ == "__main__":
    processor.run()
