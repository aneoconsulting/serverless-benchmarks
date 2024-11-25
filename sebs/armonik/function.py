from typing import cast

from sebs.faas.function import Function, FunctionConfig


class ArmoniKFunction(Function):
    @staticmethod
    def typename() -> str:
        return "ArmoniK.ArmoniKFunction"

    @staticmethod
    def deserialize(cached_config: dict) -> "ArmoniKFunction":
        from sebs.faas.function import Trigger
        from sebs.aws.triggers import LibraryTrigger, HTTPTrigger

        cfg = FunctionConfig.deserialize(cached_config["config"])
        ret = ArmoniKFunction(
            cached_config["benchmark"],
            cached_config["name"],
            cached_config["hash"],
            cfg,
        )
        for trigger in cached_config["triggers"]:
            trigger_type = cast(
                Trigger,
                {"Library": LibraryTrigger, "HTTP": HTTPTrigger}.get(trigger["type"]),
            )
            assert trigger_type, "Unknown trigger type {}".format(trigger["type"])
            ret.add_trigger(trigger_type.deserialize(trigger))
        return ret
