from dlt.common.exceptions import MissingDependencyException


def check_mcpcli_dependency_met() -> None:
    try:
        import mcpcli  # type: ignore
    except ModuleNotFoundError:
        raise MissingDependencyException(
            "dlt mcp Helpers",
            ["mcp-cli"],
        )
