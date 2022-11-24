import inspect
from typing import Any, Optional

import pytest

import dlt
from dlt.common import Decimal
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.typing import TSecretValue, is_optional_type
from dlt.common.configuration.inject import get_fun_spec, with_config
from dlt.common.configuration.specs import BaseConfiguration, RunConfiguration
from dlt.common.reflection.spec import spec_from_signature, _get_spec_name_from_f
from dlt.common.reflection.utils import get_func_def_node, get_literal_defaults

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment

_DECIMAL_DEFAULT = Decimal("0.01")
_SECRET_DEFAULT = TSecretValue("PASS")
_CONFIG_DEFAULT = RunConfiguration()


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


def test_inject_with_non_injectable_param() -> None:
    # one of parameters in signature has not valid hint and is skipped (ie. from_pipe)
    pass


def test_inject_without_spec() -> None:
    pass


def test_inject_without_spec_kw_only() -> None:
    pass


def test_inject_with_auto_namespace(environment: Any) -> None:
    environment["PIPE__VALUE"] = "test"

    @with_config(auto_namespace=True)
    def f(pipeline_name=dlt.config.value, value=dlt.secrets.value):
        assert value == "test"

    f("pipe")

    # make sure the spec is available for decorated fun
    assert get_fun_spec(f) is not None
    assert hasattr(get_fun_spec(f), "pipeline_name")


def test_inject_with_spec() -> None:
    pass


def test_inject_with_str_namespaces() -> None:
    # namespaces param is str not tuple
    pass


def test_inject_with_func_namespace() -> None:
    # function to get namespaces from the arguments is provided
    pass


def test_inject_on_class_and_methods() -> None:
    pass


def test_set_defaults_for_positional_args() -> None:
    # set defaults for positional args that are part of derived SPEC
    # set defaults for positional args that are part of provided SPEC
    pass


def test_inject_spec_remainder_in_kwargs() -> None:
    # if the wrapped func contains kwargs then all the fields from spec without matching func args must be injected in kwargs
    pass


def test_inject_spec_in_kwargs() -> None:
    # the resolved spec is injected in kwargs
    pass


def test_resolved_spec_in_kwargs_pass_through() -> None:
    # if last_config is in kwargs then use it and do not resolve it anew
    pass


def test_inject_spec_into_argument_with_spec_type() -> None:
    # if signature contains argument with type of SPEC, it gets injected there
    pass


def test_initial_spec_from_arg_with_spec_type() -> None:
    # if signature contains argument with type of SPEC, get its value to init SPEC (instead of calling the constructor())
    pass


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