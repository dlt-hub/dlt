import os
from typing import Any, Dict, Optional, Type, Union
import pytest
import time, threading
import dlt

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.inject import (
    _LAST_DLT_CONFIG,
    _ORIGINAL_ARGS,
    get_fun_spec,
    get_orig_args,
    last_config,
    with_config,
    create_resolved_partial,
)
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.providers.toml import SECRETS_TOML
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import (
    BaseConfiguration,
    GcpServiceAccountCredentialsWithoutDefaults,
    ConnectionStringCredentials,
)
from dlt.common.configuration.specs.base_configuration import (
    CredentialsConfiguration,
    configspec,
    is_secret_hint,
    is_valid_configspec_field,
)
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.reflection.spec import _get_spec_name_from_f
from dlt.common.typing import (
    StrAny,
    TSecretStrValue,
    TSecretValue,
    is_annotated,
    is_newtype_type,
    is_subclass,
)

from tests.utils import inject_providers, preserve_environ
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
    f_var_env(dlt.config.value, path="explicit path")
    f_var_env(path="explicit path", user=dlt.secrets.value)

    # none will be passed and trigger config missing
    with pytest.raises(ConfigFieldMissingException) as cfg_ex:
        f_var_env(None, path="explicit path")
    assert "user" in cfg_ex.value.traces
    assert cfg_ex.value.traces["user"][0].provider == "ExplicitValues"


def test_explicit_none(environment: Any) -> None:
    @with_config
    def f_var(user: Optional[str] = "default"):
        return user

    assert f_var(None) is None
    assert f_var() == "default"
    assert f_var(dlt.config.value) == "default"
    environment["USER"] = "env user"
    assert f_var() == "env user"
    assert f_var(None) is None
    assert f_var(dlt.config.value) == "env user"


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
    assert f_config(dlt.config.value) == "user"

    environment["PASSWORD"] = "password"
    assert f_secret() == "password"
    assert f_secret(dlt.secrets.value) == "password"


def test_dlt_literals_in_spec() -> None:
    @configspec
    class LiteralsConfiguration(BaseConfiguration):
        required_str: str = dlt.config.value
        required_int: int = dlt.config.value
        required_secret: TSecretStrValue = dlt.secrets.value
        credentials: CredentialsConfiguration = dlt.secrets.value
        optional_default: float = 1.2

    fields = {
        k: f.default
        for k, f in LiteralsConfiguration.__dataclass_fields__.items()
        if is_valid_configspec_field(f)
    }
    # make sure all special values are evaluated to None which indicate required params
    assert fields == {
        "required_str": None,
        "required_int": None,
        "required_secret": None,
        "credentials": None,
        "optional_default": 1.2,
    }
    c = LiteralsConfiguration()
    assert dict(c) == fields

    # instantiate to make sure linter does not complain
    c = LiteralsConfiguration("R", 0, TSecretStrValue("A"), ConnectionStringCredentials())
    assert dict(c) == {
        "required_str": "R",
        "required_int": 0,
        "required_secret": TSecretStrValue("A"),
        "credentials": ConnectionStringCredentials(),
        "optional_default": 1.2,
    }

    # this generates warnings
    @configspec
    class WrongLiteralsConfiguration(BaseConfiguration):
        required_int: int = dlt.secrets.value
        required_secret: TSecretStrValue = dlt.config.value
        credentials: CredentialsConfiguration = dlt.config.value


def test_dlt_literals_defaults_none() -> None:
    @with_config
    def with_optional_none(
        level: Optional[int] = dlt.config.value, aux: Optional[str] = dlt.secrets.value
    ):
        return (level, aux)

    assert with_optional_none() == (None, None)


def test_inject_from_argument_section(toml_providers: ConfigProvidersContainer) -> None:
    # `gcp_storage` is a key in `secrets.toml` and the default `credentials` section of GcpServiceAccountCredentialsWithoutDefaults must be replaced with it

    @with_config
    def f_credentials(gcp_storage: GcpServiceAccountCredentialsWithoutDefaults = dlt.secrets.value):
        # unique project name
        assert gcp_storage.project_id == "mock-project-id-gcp-storage"

    f_credentials()


def test_inject_secret_value_secret_type(environment: Any) -> None:
    @with_config
    def f_custom_secret_type(
        _dict: Dict[str, Any] = dlt.secrets.value,
        _int: int = dlt.secrets.value,
        **injection_kwargs: Any,
    ):
        # secret values were coerced into types
        assert _dict == {"a": 1}
        assert _int == 1234
        cfg = last_config(**injection_kwargs)
        spec: Type[BaseConfiguration] = cfg.__class__
        # assert that types are secret
        for f in ["_dict", "_int"]:
            f_type = spec.__dataclass_fields__[f].type
            assert is_secret_hint(f_type)
            assert cfg.get_resolvable_fields()[f] is f_type
            assert is_annotated(f_type)

    environment["_DICT"] = '{"a":1}'
    environment["_INT"] = "1234"

    f_custom_secret_type()


def test_aux_not_injected_into_kwargs() -> None:
    # only kwargs with name injection_kwargs receive aux info

    @configspec
    class AuxTest(BaseConfiguration):
        aux: str = "INFO"

    @with_config(spec=AuxTest)
    def f_no_aux(**kwargs: Any):
        assert "aux" not in kwargs
        assert _LAST_DLT_CONFIG not in kwargs
        assert _ORIGINAL_ARGS not in kwargs

    f_no_aux()


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
        print(test_sections())

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

    @with_config(sections=("test",), lock_context_on_injection=lock)
    def test_sections(value=dlt.config.value):
        return value

    os.environ["TEST__VALUE"] = "test_val"
    with inject_providers([SlowProvider()]):
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


def test_inject_with_func_section(environment: Any) -> None:
    # function to get sections from the arguments is provided

    @with_config(sections=lambda args: "dlt_" + args["name"])  # type: ignore[call-overload]
    def table_info(name, password=dlt.secrets.value):
        return password

    environment["DLT_USERS__PASSWORD"] = "pass"
    assert table_info("users") == "pass"

    @with_config(sections=lambda args: ("dlt", args["name"]))  # type: ignore[call-overload]
    def table_info_2(name, password=dlt.secrets.value):
        return password

    environment["DLT__CONTACTS__PASSWORD"] = "pass_x"
    assert table_info_2("contacts") == "pass_x"


def test_inject_on_class_and_methods(environment: Any) -> None:
    environment["AUX"] = "DEBUG"
    environment["LEVEL"] = "1"

    class AuxCallReceiver:
        @with_config
        def __call__(self, level: int = dlt.config.value, aux: str = dlt.config.value) -> Any:
            return (level, aux)

    assert AuxCallReceiver()() == (1, "DEBUG")

    class AuxReceiver:
        @with_config
        def __init__(self, level: int = dlt.config.value, aux: str = dlt.config.value) -> None:
            self.level = level
            self.aux = aux

        @with_config
        def resolve(self, level: int = dlt.config.value, aux: str = dlt.config.value) -> Any:
            return (level, aux)

    kl_ = AuxReceiver()
    assert kl_.level == 1
    assert kl_.aux == "DEBUG"

    assert kl_.resolve() == (1, "DEBUG")


@pytest.mark.skip("not implemented")
def test_set_defaults_for_positional_args() -> None:
    # set defaults for positional args that are part of derived SPEC
    # set defaults for positional args that are part of provided SPEC
    pass


def test_inject_spec_remainder_in_kwargs() -> None:
    # if the wrapped func contains kwargs then all the fields from spec without matching func args must be injected in kwargs
    @configspec
    class AuxTest(BaseConfiguration):
        level: int = None
        aux: str = "INFO"

    @with_config(spec=AuxTest)
    def f_aux(level, **injection_kwargs: Any):
        # level is in args so not added to kwargs
        assert level == 1
        assert "level" not in injection_kwargs
        # remainder in kwargs
        assert injection_kwargs["aux"] == "INFO"
        # assert _LAST_DLT_CONFIG not in kwargs
        # assert _ORIGINAL_ARGS not in kwargs

    f_aux(1)


def test_inject_spec_in_kwargs() -> None:
    @configspec
    class AuxTest(BaseConfiguration):
        aux: str = "INFO"

    @with_config(spec=AuxTest)
    def f_kw_spec(**injection_kwargs: Any):
        c = last_config(**injection_kwargs)
        assert c.aux == "INFO"
        # no args, no kwargs
        assert get_orig_args(**injection_kwargs) == ((), {})

    f_kw_spec()


def test_resolved_spec_in_kwargs_pass_through(environment: Any) -> None:
    # if last_config is in kwargs then use it and do not resolve it anew
    @configspec
    class AuxTest(BaseConfiguration):
        aux: str = "INFO"

    @with_config(spec=AuxTest)
    def init_cf(aux: str = dlt.config.value, **injection_kwargs: Any):
        assert aux == "DEBUG"
        return last_config(**injection_kwargs)

    environment["AUX"] = "DEBUG"
    c = init_cf()

    @with_config(spec=AuxTest)
    def get_cf(aux: str = dlt.config.value, last_config: AuxTest = None):
        assert aux == "DEBUG"
        assert last_config.aux == "DEBUG"
        return last_config

    # this will be ignored, last_config is regarded as resolved
    environment["AUX"] = "ERROR"
    assert get_cf(last_config=c) is c


def test_inject_spec_into_argument_with_spec_type() -> None:
    # if signature contains argument with type of SPEC, it gets injected there
    import dlt
    from dlt.common.configuration import known_sections
    from dlt.destinations.impl.dummy.configuration import DummyClientConfiguration

    @with_config(
        spec=DummyClientConfiguration,
        sections=(
            known_sections.DESTINATION,
            "dummy",
        ),
    )
    def _configure(config: DummyClientConfiguration = dlt.config.value) -> DummyClientConfiguration:
        return config

    # _configure has argument of type DummyClientConfiguration that it returns
    # this type holds resolved configuration
    c = _configure()
    assert isinstance(c, DummyClientConfiguration)


def test_initial_spec_from_arg_with_spec_type(environment: Any) -> None:
    # if signature contains argument with type of SPEC, get its value to init SPEC (instead of calling the constructor())
    @configspec
    class AuxTest(BaseConfiguration):
        level: int = None
        aux: str = "INFO"

    @with_config(spec=AuxTest)
    def init_cf(
        level: int = dlt.config.value, aux: str = dlt.config.value, init_cf: AuxTest = None
    ):
        assert level == -1
        assert aux == "DEBUG"
        # init_cf was used as init but also got resolved
        assert init_cf.aux == "DEBUG"
        return init_cf

    init_c = AuxTest(level=-1)
    environment["AUX"] = "DEBUG"
    assert init_cf(init_cf=init_c) is init_c


def test_use_most_specific_union_type(
    environment: Any, toml_providers: ConfigProvidersContainer
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
    secrets_toml = toml_providers[SECRETS_TOML]._config_doc  # type: ignore[attr-defined]
    secrets_toml["local_credentials"] = conn_str
    assert isinstance(postgres_direct(), ConnectionStringCredentials)
    assert isinstance(postgres_union(), ConnectionStringCredentials)
    secrets_toml.pop("local_credentials")
    # make sure config is successfully deleted
    with pytest.raises(ConfigFieldMissingException):
        postgres_union()
    # config_toml = toml_providers[CONFIG_TOML]._config_doc
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
