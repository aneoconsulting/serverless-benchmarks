import os
import requests
import shutil
import time
from typing import cast, Dict, List, Optional, Type, Tuple  # noqa
import subprocess
import socket

import docker

from armonik.client import ArmoniKPartitions, ArmoniKResults, ArmoniKSessions, ArmoniKTasks
from armonik.common import Partition, Session, Result, Task, TaskDefinition
from armonik.common.channel import create_channel
from grpc import RpcError

from sebs.cache import Cache
from sebs.config import SeBSConfig
from sebs.utils import LoggingHandlers, is_linux
from sebs.armonik.config import ArmoniKConfig
from sebs.armonik.storage import ArmoniKStorage
from sebs.armonik.function import ArmoniKFunction
from sebs.faas.function import Function, FunctionConfig, ExecutionResult, Trigger
from sebs.faas.storage import PersistentStorage
from sebs.faas.system import System
from sebs.benchmark import Benchmark


class Local(System):

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

    def initialize(self, config: Dict[str, str] = {}, resource_prefix: Optional[str] = None):
        self.initialize_resources(select_prefix=resource_prefix)
        channel = create_channel(self.config.resources.control_plane_url).__enter__()
        self._partition_client = ArmoniKPartitions(channel)

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
        # build function image
        _, container_uri = self.docker_client.build_base_image(
            directory, language_name, language_version, architecture, benchmark, is_cached
        )

        CONFIG_FILES = {
            "python": ["__main__.py", "handler.py", "requirements.txt"],
        }
        package_config = CONFIG_FILES[language_name]
        function_dir = os.path.join(directory, "function")
        os.makedirs(function_dir)
        # move all files to 'function' except handler.py
        for file in os.listdir(directory):
            if file not in package_config:
                file = os.path.join(directory, file)
                shutil.move(file, function_dir)

        bytes_size = os.path.getsize(directory)
        mbytes = bytes_size / 1024.0 / 1024.0
        self.logging.info("Function size {:2f} MB".format(mbytes))

        return directory, bytes_size, container_uri

    def create_function(
        self,
        code_package: Benchmark,
        func_name: str,
        container_deployment: bool,
        container_uri: str,
    ) -> "ArmoniKFunction":
        try:
            self._partition_client.get_partition(func_name)
            function = ArmoniKFunction(
                func_name,
                code_package.benchmark,
                code_package.hash,
                FunctionConfig.from_benchmark(code_package)
            )
            return function
        except RpcError:
            raise ValueError(f"Function {func_name} cannot be created from SEBS and doesn't exist in the cluster.")

    def update_function(
        self,
        function: Function,
        code_package: Benchmark,
        container_deployment: bool,
        container_uri: str,
    ):
        raise NotImplementedError("ArmoniK platform doesn't support function update.")

    def create_trigger(self, func: Function, trigger_type: Trigger.TriggerType) -> Trigger:
        from sebs.armonik.function import LibraryTrigger

        function = cast(ArmoniKFunction, func)
        trigger = LibraryTrigger(function.name, self.config.resources.control_plane_url)
        trigger.logging_handlers = self.logging_handlers
        function.add_trigger(trigger)
        self.cache_client.update_function(function)
        return trigger

    def cached_function(self, function: Function):
        from sebs.aws.triggers import LibraryTrigger

        for trigger in function.triggers(Trigger.TriggerType.LIBRARY):
            trigger.logging_handlers = self.logging_handlers
            cast(LibraryTrigger, trigger).deployment_client = self
        for trigger in function.triggers(Trigger.TriggerType.HTTP):
            trigger.logging_handlers = self.logging_handlers

    def update_function_configuration(self, function: Function, code_package: Benchmark):
        self.logging.error("Updating function configuration of ArmoniK deployment is not supported")
        raise RuntimeError("Updating function configuration of ArmoniK deployment is not supported")

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
            self.storage = ArmoniKStorage(self.config.resources.control_plane_url)
            self.storage.logging_handlers = self.logging_handlers
        else:
            self.storage.replace_existing = replace_existing
        return self.storage
