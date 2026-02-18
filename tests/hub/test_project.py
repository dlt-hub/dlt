import dlt
from tests.hub.utils import ephemeral_license


def test_imports() -> None:
    from dlthub.project import Catalog, EntityFactory, ProjectRunContext, Project, PipelineManager


def test_project_context() -> None:
    with ephemeral_license():
        context = dlt.hub.current.project.context()  # type: ignore[attr-defined,unused-ignore]
    assert context.name == "hub"
    assert dlt.hub.current.project.catalog() is not None  # type: ignore[attr-defined,unused-ignore]
    assert dlt.hub.current.project.config().name == "hub"  # type: ignore[attr-defined,unused-ignore]


def test_switch_profile() -> None:
    from dlt.hub.current import project  # type: ignore[attr-defined,unused-ignore]

    with ephemeral_license():
        ctx = project.context()
        try:
            profile = ctx.profile
            project.context().switch_profile("tests")
            assert project.context().profile == "tests"
            print(project.config().current_profile)
        finally:
            assert project.context().switch_profile(profile).profile == profile
