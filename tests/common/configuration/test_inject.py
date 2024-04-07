import os
from typing import Any, Dict, Optional, Type, Union
import pytest
import time, threading
import dlt

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.inject import (
    get_fun_spec,
    last_config,
    with_config,
    create_resolved_partial,
)
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.providers.toml import SECRETS_TOML
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import (
    BaseConfiguration,
    GcpServiceAccountCredentialsWithoutDefaults,
    ConnectionStringCredentials,
)
from dlt.common.configuration.specs.base_configuration import configspec, is_secret_hint
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.reflection.spec import _get_spec_name_from_f
from dlt.common.typing import StrAny, TSecretValue, is_newtype_type

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment, toml_providers


def test_arguments_are_explicit(environment: Any) -> None:
    @with_config
    def f_var(user=dlt.config.value, path=dlt.config.value):
        # explicit args "survive" the injection: they have precedence over env
        assert user == "explicit user"
        assert path == "explicit path"

    f_var("explicit user", "explicit path")
    environment["USER"] = "env user"
    f_var("explicit user", "explicit path")

    @with_config
    def f_var_env(user=dlt.config.value, path=dlt.config.value):
        assert user == "env user"
        assert path == "explicit path"

    # user will be injected
    f_var_env(None, path="explicit path")
    f_var_env(path="explicit path", user=None)


def test_default_values_are_resolved(environment: Any) -> None:
    @with_config
    def f_var(user=dlt.config.value, path="a/b/c"):
        assert user == "env user"
        assert path == "env path"

    environment["USER"] = "env user"
    environment["PATH"] = "env path"


def test_arguments_dlt_literal_defaults_are_required(environment: Any) -> None:
    @with_config
    def f_config(user=dlt.config.value):
        assert user is not None
        return user

    @with_config
    def f_secret(password=dlt.secrets.value):
        # explicit args "survive" the injection: they have precedence over env
        assert password is not None
        return password

    # call without user present
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        f_config()
    assert py_ex.value.fields == ["user"]
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        f_config(None)
    assert py_ex.value.fields == ["user"]

    environment["USER"] = "user"
    assert f_config() == "user"
    assert f_config(None) == "user"

    environment["PASSWORD"] = "password"
    assert f_secret() == "password"
    assert f_secret(None) == "password"


def test_inject_from_argument_section(toml_providers: ConfigProvidersContext) -> None:
    # `gcp_storage` is a key in `secrets.toml` and the default `credentials` section of GcpServiceAccountCredentialsWithoutDefaults must be replaced with it

    @with_config
    def f_credentials(gcp_storage: GcpServiceAccountCredentialsWithoutDefaults = dlt.secrets.value):
        # unique project name
        assert gcp_storage.project_id == "mock-project-id-gcp-storage"

    f_credentials()


def test_inject_secret_value_secret_type(environment: Any) -> None:
    @with_config
    def f_custom_secret_type(
        _dict: Dict[str, Any] = dlt.secrets.value, _int: int = dlt.secrets.value, **kwargs: Any
    ):
        # secret values were coerced into types
        assert _dict == {"a": 1}
        assert _int == 1234
        cfg = last_config(**kwargs)
        spec: Type[BaseConfiguration] = cfg.__class__
        # assert that types are secret
        for f in ["_dict", "_int"]:
            f_type = spec.__dataclass_fields__[f].type
            assert is_secret_hint(f_type)
            assert cfg.get_resolvable_fields()[f] is f_type
            assert is_newtype_type(f_type)

    environment["_DICT"] = '{"a":1}'
    environment["_INT"] = "1234"

    f_custom_secret_type()


@pytest.mark.skip("not implemented")
def test_inject_with_non_injectable_param() -> None:
    # one of parameters in signature has not valid hint and is skipped (ie. from_pipe)
    pass


@pytest.mark.skip("not implemented")
def test_inject_without_spec() -> None:
    pass


@pytest.mark.skip("not implemented")
def test_inject_without_spec_kw_only() -> None:
    pass


def test_inject_with_auto_section(environment: Any) -> None:
    environment["PIPE__VALUE"] = "test"

    @with_config(auto_pipeline_section=True)
    def f(pipeline_name=dlt.config.value, value=dlt.secrets.value):
        assert value == "test"

    f("pipe")

    # make sure the spec is available for decorated fun
    assert get_fun_spec(f) is not None
    assert hasattr(get_fun_spec(f), "pipeline_name")


@pytest.mark.skip("not implemented")
def test_inject_with_spec() -> None:
    pass


@pytest.mark.skip("not implemented")
def test_inject_with_sections() -> None:
    pass


def test_inject_spec_in_func_params() -> None:
    @configspec
    class TestConfig(BaseConfiguration):
        base_value: str = None

    # if any of args (ie. `init` below) is an instance of SPEC, we use it as initial value

    @with_config(spec=TestConfig)
    def test_spec_arg(base_value=dlt.config.value, init: TestConfig = None):
        return base_value

    # spec used to wrap function
    spec = get_fun_spec(test_spec_arg)
    assert spec == TestConfig
    # call function with init, should resolve even if we do not provide the base_value in config
    assert test_spec_arg(init=TestConfig(base_value="A")) == "A"


def test_inject_with_sections_and_sections_context() -> None:
    @with_config
    def no_sections(value=dlt.config.value):
        return value

    @with_config(sections=("test",))
    def test_sections(value=dlt.config.value):
        return value

    # a section context that prefers existing context
    @with_config(sections=("test",), sections_merge_style=ConfigSectionContext.prefer_existing)
    def test_sections_pref_existing(value=dlt.config.value):
        return value

    # a section that wants context like dlt resource
    @with_config(
        sections=("test", "module", "name"),
        sections_merge_style=ConfigSectionContext.resource_merge_style,
    )
    def test_sections_like_resource(value=dlt.config.value):
        return value

    os.environ["VALUE"] = "no_section"
    os.environ["TEST__VALUE"] = "test_section"
    os.environ["INJECTED__VALUE"] = "injected_section"
    os.environ["TEST__EXISTING_MODULE__NAME__VALUE"] = "resource_style_injected"

    assert no_sections() == "no_section"
    # looks in "test" section first
    assert test_sections() == "test_section"
    assert test_sections_pref_existing() == "test_section"
    assert test_sections_like_resource() == "test_section"

    with inject_section(ConfigSectionContext(sections=("injected",))):
        # the "injected" section is applied to "no_section" func that has no sections
        assert no_sections() == "injected_section"
        # but not to "test" - it won't be overridden by section context
        assert test_sections() == "test_section"
        assert test_sections_like_resource() == "test_section"
        # this one explicitly prefers existing context
        assert test_sections_pref_existing() == "injected_section"

    with inject_section(
        ConfigSectionContext(sections=("test", "existing_module", "existing_name"))
    ):
        assert test_sections_like_resource() == "resource_style_injected"


def test_partial() -> None:
    @with_config(sections=("test",))
    def test_sections(value=dlt.config.value):
        return value

    # no value in scope will fail
    with pytest.raises(ConfigFieldMissingException):
        test_sections()

    # same for partial
    with pytest.raises(ConfigFieldMissingException):
        create_resolved_partial(test_sections)

    # with value in scope partial will work
    os.environ["TEST__VALUE"] = "first_val"
    partial = create_resolved_partial(test_sections)

    # remove the value from scope and partial will work
    del os.environ["TEST__VALUE"]
    assert partial() == "first_val"

    # original func wont
    with pytest.raises(ConfigFieldMissingException):
        test_sections()

    # partial retains value
    os.environ["TEST__VALUE"] = "new_val"
    assert partial() == "first_val"
    assert test_sections() == "new_val"

    # new partial picks up new value
    new_partial = create_resolved_partial(test_sections)

    # remove the value from scope and partial will work
    del os.environ["TEST__VALUE"]
    assert new_partial() == "new_val"
    assert partial() == "first_val"


def test_base_spec() -> None:
    @configspec
    class BaseParams(BaseConfiguration):
        str_str: str = None

    @with_config(base=BaseParams)
    def f_explicit_base(str_str=dlt.config.value, opt: bool = True):
        # for testing
        assert opt is False
        return str_str

    # discovered spec should derive from TestConfig
    spec = get_fun_spec(f_explicit_base)
    assert issubclass(spec, BaseParams)
    # but derived
    assert spec != BaseParams

    # call function
    os.environ["STR_STR"] = "new_val"
    assert f_explicit_base(opt=False) == "new_val"

    # edge case, function does not take str_str but still fail because base config must resolve
    del os.environ["STR_STR"]

    @with_config(base=BaseParams)
    def f_no_base(opt: bool = True):
        raise AssertionError("never")

    with pytest.raises(ConfigFieldMissingException):
        f_no_base(opt=False)


@pytest.mark.parametrize("lock", [False, True])
@pytest.mark.parametrize("same_pool", [False, True])
def test_lock_context(lock, same_pool) -> None:
    # we create a slow provider to test locking

    class SlowProvider(EnvironProvider):
        def get_value(self, key, hint, pipeline_name, *sections):
            import time

            time.sleep(0.5)
            return super().get_value(key, hint, pipeline_name, *sections)

    ctx = ConfigProvidersContext()
    ctx.providers.clear()
    ctx.add_provider(SlowProvider())

    @with_config(sections=("test",), lock_context_on_injection=lock)
    def test_sections(value=dlt.config.value):
        return value

    os.environ["TEST__VALUE"] = "test_val"
    with Container().injectable_context(ctx):
        start = time.time()

        if same_pool:
            thread_ids = ["dlt-pool-1-1", "dlt-pool-1-2"]
        else:
            thread_ids = ["dlt-pool-5-1", "dlt-pool-20-2"]

        # simulate threads in the same pool
        thread1 = threading.Thread(target=test_sections, name=thread_ids[0])
        thread2 = threading.Thread(target=test_sections, name=thread_ids[1])

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        elapsed = time.time() - start

        # see wether there was any parallel execution going on
        # it should only lock if we're in the same pool and we want it to lock
        if lock and same_pool:
            assert elapsed > 1
        else:
            assert elapsed < 0.7


@pytest.mark.skip("not implemented")
def test_inject_with_str_sections() -> None:
    # sections param is str not tuple
    pass


@pytest.mark.skip("not implemented")
def test_inject_with_func_section() -> None:
    # function to get sections from the arguments is provided
    pass


@pytest.mark.skip("not implemented")
def test_inject_on_class_and_methods() -> None:
    pass


@pytest.mark.skip("not implemented")
def test_set_defaults_for_positional_args() -> None:
    # set defaults for positional args that are part of derived SPEC
    # set defaults for positional args that are part of provided SPEC
    pass


@pytest.mark.skip("not implemented")
def test_inject_spec_remainder_in_kwargs() -> None:
    # if the wrapped func contains kwargs then all the fields from spec without matching func args must be injected in kwargs
    pass


@pytest.mark.skip("not implemented")
def test_inject_spec_in_kwargs() -> None:
    # the resolved spec is injected in kwargs
    pass


@pytest.mark.skip("not implemented")
def test_resolved_spec_in_kwargs_pass_through() -> None:
    # if last_config is in kwargs then use it and do not resolve it anew
    pass


@pytest.mark.skip("not implemented")
def test_inject_spec_into_argument_with_spec_type() -> None:
    # if signature contains argument with type of SPEC, it gets injected there
    pass


@pytest.mark.skip("not implemented")
def test_initial_spec_from_arg_with_spec_type() -> None:
    # if signature contains argument with type of SPEC, get its value to init SPEC (instead of calling the constructor())
    pass


def test_use_most_specific_union_type(
    environment: Any, toml_providers: ConfigProvidersContext
) -> None:
    @with_config
    def postgres_union(
        local_credentials: Union[ConnectionStringCredentials, str, StrAny] = dlt.secrets.value
    ):
        return local_credentials

    @with_config
    def postgres_direct(local_credentials: ConnectionStringCredentials = dlt.secrets.value):
        return local_credentials

    conn_str = "postgres://loader:loader@localhost:5432/dlt_data"
    conn_dict = {
        "host": "localhost",
        "database": "dlt_test",
        "username": "loader",
        "password": "loader",
        "drivername": "postgresql",
    }
    conn_cred = ConnectionStringCredentials()
    conn_cred.parse_native_representation(conn_str)

    # pass explicit: str, Dict and credentials object
    assert isinstance(postgres_direct(conn_cred), ConnectionStringCredentials)
    assert isinstance(postgres_direct(conn_str), ConnectionStringCredentials)  # type: ignore[arg-type]
    assert isinstance(postgres_direct(conn_dict), ConnectionStringCredentials)  # type: ignore[arg-type]
    assert isinstance(postgres_union(conn_cred), ConnectionStringCredentials)
    assert isinstance(postgres_union(conn_str), ConnectionStringCredentials)
    assert isinstance(postgres_union(conn_dict), ConnectionStringCredentials)

    # pass via env as conn string
    environment["LOCAL_CREDENTIALS"] = conn_str
    assert isinstance(postgres_direct(), ConnectionStringCredentials)
    assert isinstance(postgres_union(), ConnectionStringCredentials)
    del environment["LOCAL_CREDENTIALS"]
    # make sure config is successfully deleted
    with pytest.raises(ConfigFieldMissingException):
        postgres_union()
    # create env with elements
    for k, v in conn_dict.items():
        environment[EnvironProvider.get_key_name(k, "local_credentials")] = v
    assert isinstance(postgres_direct(), ConnectionStringCredentials)
    assert isinstance(postgres_union(), ConnectionStringCredentials)

    environment.clear()

    # pass via toml
    secrets_toml = toml_providers[SECRETS_TOML]._toml  # type: ignore[attr-defined]
    secrets_toml["local_credentials"] = conn_str
    assert isinstance(postgres_direct(), ConnectionStringCredentials)
    assert isinstance(postgres_union(), ConnectionStringCredentials)
    secrets_toml.pop("local_credentials")
    # make sure config is successfully deleted
    with pytest.raises(ConfigFieldMissingException):
        postgres_union()
    # config_toml = toml_providers[CONFIG_TOML]._toml
    secrets_toml["local_credentials"] = {}
    for k, v in conn_dict.items():
        secrets_toml["local_credentials"][k] = v
    assert isinstance(postgres_direct(), ConnectionStringCredentials)
    assert isinstance(postgres_union(), ConnectionStringCredentials)


def test_auto_derived_spec_type_name() -> None:
    class AutoNameTest:
        @with_config
        def __init__(self, pos_par=dlt.secrets.value, /, kw_par=None) -> None:
            pass

        @classmethod
        @with_config
        def make_class(cls, pos_par, /, kw_par) -> None:
            pass

        @staticmethod
        @with_config
        def make_stuff(pos_par, /, kw_par) -> None:
            pass

        @with_config
        def stuff_test(pos_par, /, kw_par) -> None:
            pass

    # name is composed via __qualname__ of func
    assert (
        _get_spec_name_from_f(AutoNameTest.__init__)
        == "TestAutoDerivedSpecTypeNameAutoNameTestInitConfiguration"
    )
    # synthesized spec present in current module
    assert "TestAutoDerivedSpecTypeNameAutoNameTestInitConfiguration" in globals()
    # instantiate
    C: BaseConfiguration = globals()["TestAutoDerivedSpecTypeNameAutoNameTestInitConfiguration"]()
    # pos_par converted to secrets, kw_par converted to optional
    assert C.get_resolvable_fields() == {"pos_par": TSecretValue, "kw_par": Optional[Any]}
