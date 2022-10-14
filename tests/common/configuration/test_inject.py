import inspect
from typing import Any, Optional

from dlt.common import Decimal
from dlt.common.typing import TSecretValue
from dlt.common.configuration.inject import _spec_from_signature, _get_spec_name_from_f, with_config
from dlt.common.configuration.specs import BaseConfiguration, RunConfiguration


_DECIMAL_DEFAULT = Decimal("0.01")
_SECRET_DEFAULT = TSecretValue("PASS")
_CONFIG_DEFAULT = RunConfiguration()


def test_synthesize_spec_from_sig() -> None:

    # spec from typed signature without defaults

    def f_typed(p1: str, p2: Decimal, p3: Any, p4: Optional[RunConfiguration], p5: TSecretValue) -> None:
        pass

    SPEC = _spec_from_signature(f_typed.__name__, inspect.getmodule(f_typed), inspect.signature(f_typed))
    assert SPEC.p1 is None
    assert SPEC.p2 is None
    assert SPEC.p3 is None
    assert SPEC.p4 is None
    assert SPEC.p5 is None
    fields = SPEC().get_resolvable_fields()
    assert fields == {"p1": str, "p2": Decimal, "p3": Any, "p4": Optional[RunConfiguration], "p5": TSecretValue}

    # spec from typed signatures with defaults

    def f_typed_default(t_p1: str = "str", t_p2: Decimal = _DECIMAL_DEFAULT, t_p3: Any = _SECRET_DEFAULT, t_p4: RunConfiguration = _CONFIG_DEFAULT, t_p5: str = None) -> None:
        pass

    SPEC = _spec_from_signature(f_typed_default.__name__, inspect.getmodule(f_typed_default), inspect.signature(f_typed_default))
    assert SPEC.t_p1 == "str"
    assert SPEC.t_p2 == _DECIMAL_DEFAULT
    assert SPEC.t_p3 == _SECRET_DEFAULT
    assert isinstance(SPEC.t_p4, RunConfiguration)
    assert SPEC.t_p5 is None
    fields = SPEC().get_resolvable_fields()
    # Any will not assume TSecretValue type because at runtime it's a str
    # setting default as None will convert type into optional (t_p5)
    assert fields == {"t_p1": str, "t_p2": Decimal, "t_p3": str, "t_p4": RunConfiguration, "t_p5": Optional[str]}

    # spec from untyped signature

    def f_untyped(untyped_p1, untyped_p2) -> None:
        pass

    SPEC = _spec_from_signature(f_untyped.__name__, inspect.getmodule(f_untyped), inspect.signature(f_untyped))
    assert SPEC.untyped_p1 is None
    assert SPEC.untyped_p2 is None
    fields = SPEC().get_resolvable_fields()
    assert fields == {"untyped_p1": Any, "untyped_p2": Any,}

    # spec types derived from defaults


    def f_untyped_default(untyped_p1 = "str", untyped_p2 = _DECIMAL_DEFAULT, untyped_p3 = _CONFIG_DEFAULT, untyped_p4 = None) -> None:
        pass


    SPEC = _spec_from_signature(f_untyped_default.__name__, inspect.getmodule(f_untyped_default), inspect.signature(f_untyped_default))
    assert SPEC.untyped_p1 == "str"
    assert SPEC.untyped_p2 == _DECIMAL_DEFAULT
    assert isinstance(SPEC.untyped_p3, RunConfiguration)
    assert SPEC.untyped_p4 is None
    fields = SPEC().get_resolvable_fields()
    # untyped_p4 converted to Optional[Any]
    assert fields == {"untyped_p1": str, "untyped_p2": Decimal, "untyped_p3": RunConfiguration, "untyped_p4": Optional[Any]}

    # spec from signatures containing positional only and keywords only args

    def f_pos_kw_only(pos_only_1, pos_only_2: str = "default", /, *, kw_only_1, kw_only_2: int = 2) -> None:
        pass

    SPEC = _spec_from_signature(f_pos_kw_only.__name__, inspect.getmodule(f_pos_kw_only), inspect.signature(f_pos_kw_only))
    assert SPEC.pos_only_1 is None
    assert SPEC.pos_only_2 == "default"
    assert SPEC.kw_only_1 is None
    assert SPEC.kw_only_2 == 2
    fields = SPEC().get_resolvable_fields()
    assert fields == {"pos_only_1": Any, "pos_only_2": str, "kw_only_1": Any, "kw_only_2": int}

    # kw_only = True will filter in keywords only parameters
    SPEC = _spec_from_signature(f_pos_kw_only.__name__, inspect.getmodule(f_pos_kw_only), inspect.signature(f_pos_kw_only), kw_only=True)
    assert SPEC.kw_only_1 is None
    assert SPEC.kw_only_2 == 2
    assert not hasattr(SPEC, "pos_only_1")
    fields = SPEC().get_resolvable_fields()
    assert fields == {"kw_only_1": Any, "kw_only_2": int}

    def f_variadic(var_1: str, *args, kw_var_1: str, **kwargs) -> None:
        pass

    SPEC = _spec_from_signature(f_variadic.__name__, inspect.getmodule(f_variadic), inspect.signature(f_variadic))
    assert SPEC.var_1 is None
    assert SPEC.kw_var_1 is None
    assert not hasattr(SPEC, "args")
    fields = SPEC().get_resolvable_fields()
    assert fields == {"var_1": str, "kw_var_1": str}


def test_inject_with_non_injectable_param() -> None:
    # one of parameters in signature has not valid hint and is skipped (ie. from_pipe)
    pass


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
        def __init__(self, pos_par, /, kw_par) -> None:
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
    assert C.get_resolvable_fields() == {"pos_par": Any, "kw_par": Any}