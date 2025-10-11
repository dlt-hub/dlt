import dlt


def test_imports() -> None:
    from dlthub.project import Catalog, EntityFactory, ProjectRunContext, Project, PipelineManager


def test_project_context() -> None:
    context = dlt.hub.current.project.context()
    assert context.name == "hub"
    assert dlt.hub.current.project.catalog() is not None
    assert dlt.hub.current.project.config().name == "hub"


def test_switch_profile() -> None:
    from dlt.hub.current import project

    ctx = project.context()
    try:
        profile = ctx.profile
        project.context().switch_profile("tests")
        assert project.context().profile == "tests"
        print(project.config().current_profile)
    finally:
        assert project.context().switch_profile(profile).profile == profile
