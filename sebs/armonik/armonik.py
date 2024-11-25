import os
import subprocess
from typing import cast, Dict, List, Optional, Type, Tuple  # noqa

import docker

from datetime import timedelta

from armonik.client import ArmoniKPartitions, ArmoniKResults, ArmoniKSessions, ArmoniKTasks, ArmoniKEvents
from armonik.common import TaskOptions
from armonik.common.channel import create_channel
from grpc import RpcError

from sebs.cache import Cache
from sebs.config import SeBSConfig
from sebs.utils import LoggingHandlers
from sebs.armonik.config import ArmoniKConfig
from sebs.armonik.storage import ArmoniKStorage
from sebs.armonik.container import ArmoniKContainer
from sebs.armonik.function import ArmoniKFunction
from sebs.faas.container import DockerContainer
from sebs.faas.function import Function, FunctionConfig, ExecutionResult, Trigger
from sebs.faas.storage import PersistentStorage
from sebs.faas.system import System
from sebs.benchmark import Benchmark


class ArmoniK(System):

    _config: ArmoniKConfig

    @staticmethod
    def name():
        return "armonik"

    @staticmethod
    def typename():
        return "ArmoniK"

    @staticmethod
    def function_type() -> "Type[Function]":
        return ArmoniKFunction

    @property
    def config(self) -> ArmoniKConfig:
        return self._config

    def __init__(
        self,
        sebs_config: SeBSConfig,
        config: ArmoniKConfig,
        cache_client: Cache,
        docker_client: docker.client,
        logger_handlers: LoggingHandlers,
    ):
        super().__init__(sebs_config, cache_client, docker_client)
        self.logging_handlers = logger_handlers
        self._config = config
        self.storage: Optional[ArmoniKStorage] = None
        self.container_client = ArmoniKContainer(self.system_config, self.docker_client)

    def initialize(self, config: Dict[str, str] = {}, resource_prefix: Optional[str] = None):
        channel = create_channel(self.config.resources.control_plane_url).__enter__()
        self.partition_client = ArmoniKPartitions(channel)
        self.result_client = ArmoniKResults(channel)
        self.task_client = ArmoniKTasks(channel)
        self.session_client = ArmoniKSessions(channel)
        self.event_client = ArmoniKEvents(channel)
        self.session_id = self.session_client.create_session(
            default_task_options=TaskOptions(
                max_duration=timedelta(minutes=5),
                max_retries=1,
                priority=1
            ),
            partition_ids=["default"]
        )
        self.initialize_resources(select_prefix=resource_prefix)

    def get_task_client(self):
        return self.task_client

    def get_result_client(self):
        return self.result_client

    def get_event_client(self):
        return self.event_client

    def get_session_id(self):
        return self.session_id

    def package_code(
        self,
        directory: str,
        language_name: str,
        language_version: str,
        architecture: str,
        benchmark: str,
        is_cached: bool,
        container_deployment: bool,
    ) -> Tuple[str, int, str]:

        _, image_uri = self.container_client.build_base_image(
            directory, language_name, language_version, architecture, benchmark, is_cached
        )

        benchmark_archive = os.path.join(directory, f"{benchmark}.zip")
        subprocess.run(
            ["zip", "-qu", "-r9", benchmark_archive, "build"], stdout=subprocess.DEVNULL, cwd=directory
        )
        self.logging.info(f"Created {benchmark_archive} archive")
        bytes_size = os.path.getsize(benchmark_archive)
        self.logging.info("Zip archive size {:2f} MB".format(bytes_size / 1024.0 / 1024.0))
        return benchmark_archive, bytes_size, image_uri

    def create_function(
        self,
        code_package: Benchmark,
        func_name: str,
        container_deployment: bool,
        container_uri: str,
    ) -> "ArmoniKFunction":
        try:
            self.partition_client.get_partition(self.get_function_partition(func_name))
            function = ArmoniKFunction(
                code_package.benchmark,
                func_name,
                code_package.hash,
                FunctionConfig.from_benchmark(code_package)
            )
            return function
        except RpcError:
            raise ValueError(f"Function {func_name} cannot be created from SEBS and doesn't exist in the cluster.")

    def get_function_partition(self, func_name: str) -> str:
        return func_name.replace(".", "-")

    def update_function(
        self,
        function: Function,
        code_package: Benchmark,
        container_deployment: bool,
        container_uri: str,
    ):
        # raise NotImplementedError("ArmoniK platform doesn't support function update.")
        pass

    def create_trigger(self, func: Function, trigger_type: Trigger.TriggerType) -> Trigger:
        from sebs.armonik.triggers import LibraryTrigger

        function = cast(ArmoniKFunction, func)
        trigger = LibraryTrigger(function.name, self)
        trigger.logging_handlers = self.logging_handlers
        function.add_trigger(trigger)
        # self.cache_client.update_function(function)
        return trigger

    def cached_function(self, function: Function):
        from sebs.aws.triggers import LibraryTrigger

        for trigger in function.triggers(Trigger.TriggerType.LIBRARY):
            trigger.logging_handlers = self.logging_handlers
            cast(LibraryTrigger, trigger).deployment_client = self
        for trigger in function.triggers(Trigger.TriggerType.HTTP):
            trigger.logging_handlers = self.logging_handlers

    def update_function_configuration(self, function: Function, code_package: Benchmark):
        #self.logging.error("Updating function configuration of ArmoniK deployment is not supported")
        #raise RuntimeError("Updating function configuration of ArmoniK deployment is not supported")
        pass

    def download_metrics(
        self,
        function_name: str,
        start_time: int,
        end_time: int,
        requests: Dict[str, ExecutionResult],
        metrics: dict,
    ):
        pass

    def enforce_cold_start(self, functions: List[Function], code_package: Benchmark):
        raise NotImplementedError()

    @staticmethod
    def default_function_name(code_package: Benchmark) -> str:
        # Create function name
        func_name = "{}-{}-{}".format(
            code_package.benchmark, code_package.language_name, code_package.language_version
        )
        return func_name

    @staticmethod
    def format_function_name(func_name: str) -> str:
        return func_name

    def get_storage(self, replace_existing: bool = False) -> PersistentStorage:
        if not self.storage:
            self.storage = ArmoniKStorage(
                cache_client=self.cache_client,
                resources=self.config.resources,
                replace_existing=replace_existing,
                result_client=self.result_client,
                session_id=self.session_id
            )
            self.storage.logging_handlers = self.logging_handlers
        else:
            self.storage.replace_existing = replace_existing
        return self.storage

    def shutdown(self) -> None:
        super().shutdown()
