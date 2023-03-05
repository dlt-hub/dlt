import os
from typing import Any, Dict, Optional, Type, Union
import pytest

import dlt

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.inject import get_fun_spec, last_config, with_config
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.providers.toml import CONFIG_TOML, SECRETS_TOML, TomlProvider
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import BaseConfiguration, GcpClientCredentials
from dlt.common.configuration.specs import PostgresCredentials
from dlt.common.configuration.specs.base_configuration import is_secret_hint
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

    # `gcp_storage` is a key in `secrets.toml` and the default `credentials` section of GcpClientCredentials must be replaced with it

    @with_config
    def f_credentials(gcp_storage: GcpClientCredentials = dlt.secrets.value):
        # unique project name
        assert gcp_storage.project_id == "mock-project-id-gcp-storage"

    f_credentials()


def test_inject_secret_value_secret_type(environment: Any) -> None:

    @with_config
    def f_custom_secret_type(_dict: Dict[str, Any] = dlt.secrets.value, _int: int = dlt.secrets.value, **kwargs: Any):
        # secret values were coerced into types
        assert _dict == {"a":1}
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


def test_inject_with_sections_and_sections_context() -> None:

    @with_config
    def no_sections(value=dlt.config.value):
        return value

    @with_config(sections=("test", ))
    def test_sections(value=dlt.config.value):
        return value

    # a section context that prefers existing context
    @with_config(sections=("test", ), sections_merge_style=ConfigSectionContext.prefer_existing)
    def test_sections_pref_existing(value=dlt.config.value):
        return value


    # a section that wants context like dlt resource
    @with_config(sections=("test", "module", "name"), sections_merge_style=ConfigSectionContext.resource_merge_style)
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

    with inject_section(ConfigSectionContext(sections=("injected", ))):
        # the "injected" section is applied to "no_section" func that has no sections
        assert no_sections() == "injected_section"
        # but not to "test" - it won't be overridden by section context
        assert test_sections() == "test_section"
        assert test_sections_like_resource() == "test_section"
        # this one explicitly prefers existing context
        assert test_sections_pref_existing() == "injected_section"

    with inject_section(ConfigSectionContext(sections=("test", "existing_module", "existing_name"))):
        assert test_sections_like_resource() == "resource_style_injected"


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


def test_use_most_specific_union_type(environment: Any, toml_providers: ConfigProvidersContext) -> None:

    @with_config
    def postgres_union(local_credentials: Union[PostgresCredentials, str, StrAny] = dlt.secrets.value):
        return local_credentials

    @with_config
    def postgres_direct(local_credentials: PostgresCredentials = dlt.secrets.value):
        return local_credentials

    conn_str = "postgres://loader:loader@localhost:5432/dlt_data"
    conn_dict = {"host": "localhost", "database": "dlt_test", "username": "loader", "password": "loader"}
    conn_cred = PostgresCredentials()
    conn_cred.parse_native_representation(conn_str)

    # pass explicit: str, Dict and credentials object
    assert isinstance(postgres_direct(conn_cred), PostgresCredentials)
    assert isinstance(postgres_direct(conn_str), PostgresCredentials)
    assert isinstance(postgres_direct(conn_dict), PostgresCredentials)
    assert isinstance(postgres_union(conn_cred), PostgresCredentials)
    assert isinstance(postgres_union(conn_str), PostgresCredentials)
    assert isinstance(postgres_union(conn_dict), PostgresCredentials)

    # pass via env as conn string
    environment["LOCAL_CREDENTIALS"] = conn_str
    assert isinstance(postgres_direct(), PostgresCredentials)
    assert isinstance(postgres_union(), PostgresCredentials)
    del environment["LOCAL_CREDENTIALS"]
    # make sure config is successfully deleted
    with pytest.raises(ConfigFieldMissingException):
        postgres_union()
    # create env with elements
    for k, v in conn_dict.items():
        environment[EnvironProvider.get_key_name(k, "local_credentials")] = v
    assert isinstance(postgres_direct(), PostgresCredentials)
    assert isinstance(postgres_union(), PostgresCredentials)

    environment.clear()

    # pass via toml
    secrets_toml = toml_providers[SECRETS_TOML]._toml
    secrets_toml["local_credentials"] = conn_str
    assert isinstance(postgres_direct(), PostgresCredentials)
    assert isinstance(postgres_union(), PostgresCredentials)
    secrets_toml.pop("local_credentials")
    # make sure config is successfully deleted
    with pytest.raises(ConfigFieldMissingException):
        postgres_union()
    # config_toml = toml_providers[CONFIG_TOML]._toml
    secrets_toml["local_credentials"] = {}
    for k, v in conn_dict.items():
        secrets_toml["local_credentials"][k] = v
    assert isinstance(postgres_direct(), PostgresCredentials)
    assert isinstance(postgres_union(), PostgresCredentials)


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
    assert _get_spec_name_from_f(AutoNameTest.__init__) == "TestAutoDerivedSpecTypeNameAutoNameTestInitConfiguration"
    # synthesized spec present in current module
    assert "TestAutoDerivedSpecTypeNameAutoNameTestInitConfiguration" in globals()
    # instantiate
    C: BaseConfiguration = globals()["TestAutoDerivedSpecTypeNameAutoNameTestInitConfiguration"]()
    # pos_par converted to secrets, kw_par converted to optional
    assert C.get_resolvable_fields() == {"pos_par": TSecretValue, "kw_par": Optional[Any]}