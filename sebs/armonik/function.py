from __future__ import annotations

from typing import cast, Optional
from dataclasses import dataclass

from sebs.benchmark import Benchmark
from sebs.faas.function import Function, FunctionConfig, Runtime
from sebs.storage.config import MinioConfig


@dataclass
class ArmoniKFunctionConfig(FunctionConfig):

    # FIXME: merge with higher level abstraction for images
    docker_image: str = ""
    docker_tage: str = ""

    @staticmethod
    def deserialize(data: dict) -> ArmoniKFunctionConfig:
        keys = list(ArmoniKFunctionConfig.__dataclass_fields__.keys())
        data = {k: v for k, v in data.items() if k in keys}
        data["runtime"] = Runtime.deserialize(data["runtime"])
        return ArmoniKFunctionConfig(**data)

    def serialize(self) -> dict:
        return self.__dict__

    @staticmethod
    def from_benchmark(benchmark: Benchmark) -> ArmoniKFunctionConfig:
        return super(ArmoniKFunctionConfig, ArmoniKFunctionConfig)._from_benchmark(
            benchmark, ArmoniKFunctionConfig
        )


class ArmoniKFunction(Function):
    def __init__(
        self, name: str, benchmark: str, code_package_hash: str, cfg: ArmoniKFunctionConfig
    ):
        super().__init__(benchmark, name, code_package_hash, cfg)

    @property
    def config(self) -> ArmoniKFunctionConfig:
        return cast(ArmoniKFunctionConfig, self._cfg)

    @property
    def full_spec(self) -> dict:
        return {
            "replicas": 0,
            "polling_agent": {
                "limits": {"cpu": "2000m", "memory": "2048Mi"},
                "requests": {"cpu": "50m", "memory": "50Mi"},
            },
            "worker": [
                {
                    "image": self._cfg.docker_image,
                    "tag": self._cfg.docker_tag,
                    "limits": {"cpu": "2000m", "memory": f"{self._cfg.memory}Mi"},
                    "requests": {"cpu": "2000m", "memory": f"{self._cfg.memory}Mi"},
                }
            ],
            "hpa": {
                "type": "prometheus",
                "polling_interval": 15,
                "cooldown_period": 300,
                "min_replica_count": 0,
                "max_replica_count": 5,
                "behavior": {
                    "restore_to_original_replica_count": True,
                    "stabilization_window_seconds": 300,
                    "type": "Percent",
                    "value": 100,
                    "period_seconds": 15,
                },
                "triggers": [{"type": "prometheus", "threshold": 2}],
            },
        }

    @staticmethod
    def typename() -> str:
        return "ArmoniK.Function"

    def serialize(self) -> dict:
        return {**super().serialize(), "config": self._cfg.serialize()}

    @staticmethod
    def deserialize(cached_config: dict) -> ArmoniKFunction:
        from sebs.faas.function import Trigger
        from sebs.armonik.triggers import LibraryTrigger

        cfg = ArmoniKFunctionConfig.deserialize(cached_config["config"])
        ret = ArmoniKFunction(
            cached_config["name"], cached_config["benchmark"], cached_config["hash"], cfg
        )
        for trigger in cached_config["triggers"]:
            trigger_type = cast(
                Trigger,
                {"Library": LibraryTrigger},
            )
            assert trigger_type, "Unknown trigger type {}".format(trigger["type"])
            ret.add_trigger(trigger_type.deserialize(trigger))
        return ret
