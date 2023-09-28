from typing import MutableMapping


def mock_image_env(environment: MutableMapping[str, str]) -> None:
    environment["COMMIT_SHA"] = "192891"
    environment["IMAGE_VERSION"] = "scale/v:112"


def mock_pod_env(environment: MutableMapping[str, str]) -> None:
    environment["KUBE_NODE_NAME"] = "node_name"
    environment["KUBE_POD_NAME"] = "pod_name"
    environment["KUBE_POD_NAMESPACE"] = "namespace"


def mock_github_env(environment: MutableMapping[str, str]) -> None:
    environment["CODESPACES"] = "true"
    environment["GITHUB_USER"] = "rudolfix"
    environment["GITHUB_REPOSITORY"] = "dlt-hub/beginners-workshop-2022"
    environment["GITHUB_REPOSITORY_OWNER"] = "dlt-hub"
