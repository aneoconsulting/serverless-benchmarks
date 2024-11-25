from typing import Tuple

from sebs.faas.container import DockerContainer


class ArmoniKContainer(DockerContainer):
    @staticmethod
    def name() -> str:
        return "armonik"

    @staticmethod
    def typename() -> str:
        return "ArmoniK.Container"

    def registry_name(
        self, benchmark: str, language_name: str, language_version: str, architecture: str
    ) -> Tuple[str, str, str, str]:

        registry_name = "Docker Hub"
        repository_name = self.system_config.docker_repository()
        image_tag = self.system_config.benchmark_image_tag(
            self.name(), benchmark, language_name, language_version, architecture
        )
        image_uri = f"{repository_name}:{image_tag}"

        return registry_name, repository_name, image_tag, image_uri
