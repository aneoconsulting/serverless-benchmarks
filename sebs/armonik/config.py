from typing import cast, Optional, Set

from sebs.cache import Cache
from sebs.faas.config import Config, Credentials, Resources
from sebs.utils import LoggingHandlers


class ArmoniKCredentials(Credentials):
    def serialize(self) -> dict:
        return {}

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Credentials:
        return ArmoniKCredentials()


class ArmoniKResources(Resources):
    def __init__(self, control_plane_url: Optional[str] = None):
        super().__init__(name="armonik")
        self._control_plane_url = control_plane_url

    @property
    def control_plane_url(self):
        return self._control_plane_url

    def serialize(self) -> dict:
        return {"control_plane_url": self._control_plane_url}

    @staticmethod
    def initialize(res: Resources, config: dict):
        resources = cast(ArmoniKResources, res)
        # Check for new config
        if "control_plane_url" in config:
            resources._control_plane_url = config["control_plane_url"]

    def update_cache(self, cache: Cache):
        super().update_cache(cache)
        cache.update_config(
            val=self._control_plane_url, keys=["armonik", "resources", "control_plane_url"]
        )

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Resources:
        ret = ArmoniKResources()

        cached_config = cache.get_config("armonik")
        # Load cached values
        if cached_config and "resources" in cached_config:
            ArmoniKResources.initialize(ret, cached_config["resources"])
            ret.logging_handlers = handlers
            ret.logging.info("Using cached resources for ArmoniK")
        else:
            # Check for new config
            ret.logging_handlers = handlers
            ArmoniKResources.initialize(ret, config)

        return ret


class ArmoniKConfig(Config):
    def __init__(self):
        super().__init__(name="armonik")
        self._credentials = ArmoniKCredentials()
        self._resources = ArmoniKResources()

    @staticmethod
    def typename() -> str:
        return "ArmoniK.Config"

    @staticmethod
    def initialize(cfg: Config, dct: dict):
        pass

    @property
    def credentials(self) -> ArmoniKCredentials:
        return self._credentials

    @property
    def resources(self) -> ArmoniKResources:
        return self._resources

    @resources.setter
    def resources(self, val: ArmoniKResources):
        self._resources = val

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Config:
        config_obj = ArmoniKConfig()
        config_obj.resources = cast(
            ArmoniKResources, ArmoniKResources.deserialize(config, cache, handlers)
        )
        config_obj.logging_handlers = handlers
        return config_obj

    def serialize(self) -> dict:
        return {"name": "armonik", "resources": self._resources.serialize()}

    def update_cache(self, cache: Cache):
        self.resources.update_cache(cache)