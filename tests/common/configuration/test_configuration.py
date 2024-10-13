import pytest
import datetime  # noqa: I251
from unittest.mock import patch
from typing import (
    Any,
    Dict,
    Final,
    Generic,
    List,
    Literal,
    Mapping,
    MutableMapping,
    NewType,
    Optional,
    Type,
    Union,
)
from typing_extensions import Annotated, TypeVar

from dlt.common import json, pendulum, Decimal, Wei
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs.base_configuration import NotResolved, is_hint_not_resolvable
from dlt.common.configuration.specs.gcp_credentials import (
    GcpServiceAccountCredentialsWithoutDefaults,
)
from dlt.common.utils import custom_environ, get_exception_trace, get_exception_trace_chain
from dlt.common.typing import (
    AnyType,
    CallableAny,
    ConfigValue,
    DictStrAny,
    SecretSentinel,
    StrAny,
    TSecretStrValue,
    TSecretValue,
    extract_inner_type,
)
from dlt.common.configuration.exceptions import (
    ConfigFieldMissingTypeHintException,
    ConfigFieldTypeHintNotSupported,
    InvalidNativeValue,
    LookupTrace,
    ValueNotSecretException,
    UnmatchedConfigHintResolversException,
)
from dlt.common.configuration import (
    configspec,
    ConfigFieldMissingException,
    ConfigValueCannotBeCoercedException,
    resolve,
    is_valid_hint,
    resolve_type,
)
from dlt.common.configuration.specs import (
    BaseConfiguration,
    RuntimeConfiguration,
    ConnectionStringCredentials,
)
from dlt.common.configuration.providers import environ as environ_provider, toml
from dlt.common.configuration.utils import (
    get_resolved_traces,
    ResolvedValueTrace,
    serialize_value,
    deserialize_value,
    add_config_dict_to_env,
    add_config_to_env,
)
from dlt.common.pipeline import TRefreshMode

from dlt.destinations.impl.postgres.configuration import PostgresCredentials
from tests.utils import preserve_environ
from tests.common.configuration.utils import (
    MockProvider,
    CoercionTestConfiguration,
    COERCIONS,
    SecretCredentials,
    WithCredentialsConfiguration,
    WrongConfiguration,
    SecretConfiguration,
    SectionedConfiguration,
    environment,
    mock_provider,
    env_provider,
    reset_resolved_traces,
)

INVALID_COERCIONS = {
    # 'STR_VAL': 'test string',  # string always OK
    "int_val": "a12345",
    "bool_val": "not_bool",  # bool overridden by string - that is the most common problem
    "list_val": {"2": 1, "3": 3.0},
    "dict_val": "{'a': 1, 'b', '2'}",
    "bytes_val": "Hello World!",
    "float_val": "invalid",
    "tuple_val": "{1:2}",
    "date_val": "01 May 2022",
    "dec_val": True,
}

EXCEPTED_COERCIONS = {
    # allows to use int for float
    "float_val": 10,
    # allows to use float for str
    "str_val": 10.0,
}

COERCED_EXCEPTIONS = {
    # allows to use int for float
    "float_val": 10.0,
    # allows to use float for str
    "str_val": "10.0",
}


@configspec
class VeryWrongConfiguration(WrongConfiguration):
    pipeline_name: str = "Some Name"
    str_val: str = ""
    int_val: int = None
    log_color: str = "1"  # type: ignore


@configspec
class ConfigurationWithOptionalTypes(RuntimeConfiguration):
    pipeline_name: str = "Some Name"

    str_val: Optional[str] = None
    int_val: Optional[int] = None
    bool_val: bool = True


@configspec
class ProdConfigurationWithOptionalTypes(ConfigurationWithOptionalTypes):
    prod_val: str = "prod"


@configspec
class MockProdConfiguration(RuntimeConfiguration):
    pipeline_name: str = "comp"


@configspec
class FieldWithNoDefaultConfiguration(RuntimeConfiguration):
    no_default: str = None


@configspec
class InstrumentedConfiguration(BaseConfiguration):
    head: str = None
    tube: List[str] = None
    heels: str = None

    def to_native_representation(self) -> Any:
        return self.head + ">" + ">".join(self.tube) + ">" + self.heels

    def parse_native_representation(self, native_value: Any) -> None:
        if not isinstance(native_value, str):
            raise ValueError(native_value)
        parts = native_value.split(">")
        self.head = parts[0]
        self.heels = parts[-1]
        self.tube = parts[1:-1]
        if not self.is_partial():
            self.resolve()

    def on_resolved(self) -> None:
        if self.head > self.heels:
            raise RuntimeError("Head over heels")


@configspec
class EmbeddedConfiguration(BaseConfiguration):
    default: str = None
    instrumented: InstrumentedConfiguration = None
    sectioned: SectionedConfiguration = None


@configspec
class EmbeddedOptionalConfiguration(BaseConfiguration):
    instrumented: Optional[InstrumentedConfiguration] = None


@configspec
class EmbeddedSecretConfiguration(BaseConfiguration):
    secret: SecretConfiguration = None


@configspec
class NonTemplatedNestedTypesConfiguration(BaseConfiguration):
    list_val: list = None  # type: ignore[type-arg]
    tuple_val: tuple = None  # type: ignore[type-arg]
    dict_val: dict = None  # type: ignore[type-arg]


@configspec
class DynamicConfigA(BaseConfiguration):
    field_for_a: str = None


@configspec
class DynamicConfigB(BaseConfiguration):
    field_for_b: str = None


@configspec
class DynamicConfigC(BaseConfiguration):
    field_for_c: str = None


@configspec
class ConfigWithDynamicType(BaseConfiguration):
    discriminator: str = None
    embedded_config: BaseConfiguration = None

    @resolve_type("embedded_config")
    def resolve_embedded_type(self) -> Type[BaseConfiguration]:
        if self.discriminator == "a":
            return DynamicConfigA
        elif self.discriminator == "b":
            return DynamicConfigB
        return BaseConfiguration


@configspec
class ConfigWithInvalidDynamicType(BaseConfiguration):
    @resolve_type("a")
    def resolve_a_type(self) -> Type[BaseConfiguration]:
        return DynamicConfigA

    @resolve_type("b")
    def resolve_b_type(self) -> Type[BaseConfiguration]:
        return DynamicConfigB

    @resolve_type("c")
    def resolve_c_type(self) -> Type[BaseConfiguration]:
        return DynamicConfigC


@configspec
class SubclassConfigWithDynamicType(ConfigWithDynamicType):
    is_number: bool = None
    dynamic_type_field: Any = None

    @resolve_type("embedded_config")
    def resolve_embedded_type(self) -> Type[BaseConfiguration]:
        if self.discriminator == "c":
            return DynamicConfigC
        return super().resolve_embedded_type()

    @resolve_type("dynamic_type_field")
    def resolve_dynamic_type_field(self) -> Type[Union[int, str]]:
        if self.is_number:
            return int
        return str


@configspec
class ConfigWithLiteralField(BaseConfiguration):
    refresh: TRefreshMode = None


LongInteger = NewType("LongInteger", int)
FirstOrderStr = NewType("FirstOrderStr", str)
SecondOrderStr = NewType("SecondOrderStr", FirstOrderStr)


def test_initial_config_state() -> None:
    assert BaseConfiguration.__is_resolved__ is False
    assert BaseConfiguration.__section__ is None
    c = BaseConfiguration()
    assert c.__is_resolved__ is False
    assert c.is_resolved() is False
    # base configuration has no resolvable fields so is never partial
    assert c.is_partial() is False


def test_set_default_config_value(environment: Any) -> None:
    # set from init method
    c = resolve.resolve_configuration(
        InstrumentedConfiguration(head="h", tube=["a", "b"], heels="he")
    )
    assert c.to_native_representation() == "h>a>b>he"
    # set from native form
    c = resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value="h>a>b>he")
    assert c.head == "h"
    assert c.tube == ["a", "b"]
    assert c.heels == "he"
    # set from dictionary
    c = resolve.resolve_configuration(
        InstrumentedConfiguration(),
        explicit_value={"head": "h", "tube": ["tu", "be"], "heels": "xhe"},
    )
    assert c.to_native_representation() == "h>tu>be>xhe"


def test_explicit_values(environment: Any) -> None:
    # explicit values override the environment and all else
    environment["PIPELINE_NAME"] = "env name"
    environment["CREATED_VAL"] = "12837"
    # set explicit values and allow partial config
    c = resolve.resolve_configuration(
        CoercionTestConfiguration(),
        explicit_value={
            "pipeline_name": "initial name",
            "none_val": type(environment),
            "bytes_val": b"str",
        },
        accept_partial=True,
    )
    # explicit
    assert c.pipeline_name == "initial name"
    # explicit (no env)
    assert c.bytes_val == b"str"
    assert c.none_val == type(environment)

    # unknown field in explicit value dict is ignored
    c = resolve.resolve_configuration(
        CoercionTestConfiguration(), explicit_value={"created_val": "3343"}, accept_partial=True
    )
    assert "created_val" not in c


def test_explicit_values_false_when_bool() -> None:
    # values like 0, [], "" all coerce to bool False
    c = resolve.resolve_configuration(
        InstrumentedConfiguration(), explicit_value={"head": "", "tube": [], "heels": ""}
    )
    assert c.head == ""
    assert c.tube == []
    assert c.heels == ""


def test_explicit_embedded_config(environment: Any) -> None:
    instr_explicit = InstrumentedConfiguration(head="h", tube=["tu", "be"], heels="xhe")

    environment["INSTRUMENTED__HEAD"] = "hed"
    c = resolve.resolve_configuration(
        EmbeddedConfiguration(default="X", sectioned=SectionedConfiguration(password="S")),
        explicit_value={"instrumented": instr_explicit},
    )

    # explicit value will be part of the resolved configuration
    assert c.instrumented is instr_explicit
    # configuration was injected from env
    assert c.instrumented.head == "hed"

    # the same but with resolved
    instr_explicit = InstrumentedConfiguration(head="h", tube=["tu", "be"], heels="xhe")
    instr_explicit.resolve()
    c = resolve.resolve_configuration(
        EmbeddedConfiguration(default="X", sectioned=SectionedConfiguration(password="S")),
        explicit_value={"instrumented": instr_explicit},
    )
    assert c.instrumented is instr_explicit
    # but configuration is not injected
    assert c.instrumented.head == "h"


def test_default_values(environment: Any) -> None:
    # explicit values override the environment and all else
    environment["PIPELINE_NAME"] = "env name"
    environment["CREATED_VAL"] = "12837"
    # set default values and allow partial config
    default = CoercionTestConfiguration()
    default.pipeline_name = "initial name"
    default.none_val = type(environment)  # type: ignore[assignment]
    default.bytes_val = b"str"
    c = resolve.resolve_configuration(default, accept_partial=True)
    # env over default
    assert c.pipeline_name == "env name"
    # default (no env)
    assert c.bytes_val == b"str"
    # default not serializable object
    assert c.none_val == type(environment)


def test_raises_on_final_value_change(environment: Any) -> None:
    @configspec
    class FinalConfiguration(BaseConfiguration):
        pipeline_name: Final[str] = "comp"

    c = resolve.resolve_configuration(FinalConfiguration())
    assert dict(c) == {"pipeline_name": "comp"}

    environment["PIPELINE_NAME"] = "env name"
    c = resolve.resolve_configuration(FinalConfiguration())
    # config providers are ignored for final fields
    assert c.pipeline_name == "comp"

    @configspec
    class FinalConfiguration2(BaseConfiguration):
        pipeline_name: Final[str] = None

    c2 = resolve.resolve_configuration(FinalConfiguration2())
    assert dict(c2) == {"pipeline_name": None}


def test_explicit_native_always_skips_resolve(environment: Any) -> None:
    # make the instance sectioned so it can read from INSTRUMENTED
    with patch.object(InstrumentedConfiguration, "__section__", "ins"):
        # explicit native representations skips resolve
        environment["INS__HEELS"] = "xhe"
        c = resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value="h>a>b>he")
        assert c.heels == "he"

        # normal resolve (heels from env)
        c = InstrumentedConfiguration(head="h", tube=["tu", "be"])
        c = resolve.resolve_configuration(c)
        assert c.heels == "xhe"

        # explicit representation
        environment["INS"] = "h>a>b>he"
        c = resolve.resolve_configuration(
            InstrumentedConfiguration(),
            explicit_value={"head": "h", "tube": ["tu", "be"], "heels": "uhe"},
        )
        assert c.heels == "uhe"

        # also the native explicit value
        c = resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value="h>a>b>uhe")
        assert c.heels == "uhe"


def test_lookup_native_config_value_if_config_section(environment: Any) -> None:
    # mock the __section__ to enable the query
    with patch.object(InstrumentedConfiguration, "__section__", "snake"):
        c = InstrumentedConfiguration(head="h", tube=["a", "b"], heels="he")
        # provide the native value
        environment["SNAKE"] = "h>tu>be>xhe"
        c = resolve.resolve_configuration(c)
        # check if the native value loaded
        assert c.heels == "xhe"


def test_skip_lookup_native_config_value_if_no_config_section(environment: Any) -> None:
    # the INSTRUMENTED is not looked up because InstrumentedConfiguration has no section
    with custom_environ({"INSTRUMENTED": "he>tu>u>be>h"}):
        with pytest.raises(ConfigFieldMissingException) as py_ex:
            resolve.resolve_configuration(
                EmbeddedConfiguration(),
                explicit_value={"default": "set", "sectioned": {"password": "pwd"}},
            )
        assert py_ex.value.spec_name == "InstrumentedConfiguration"
        assert py_ex.value.fields == ["head", "tube", "heels"]

    # also non embedded InstrumentedConfiguration will not be resolved - there's no way to infer initial key


def test_invalid_native_config_value() -> None:
    # 2137 cannot be parsed and also is not a dict that can initialize the fields
    with pytest.raises(InvalidNativeValue) as py_ex:
        resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value=2137)
    assert py_ex.value.spec is InstrumentedConfiguration
    assert py_ex.value.native_value_type is int
    assert py_ex.value.embedded_sections == ()


def test_maybe_use_explicit_value() -> None:
    # pass through dict and configs
    c = ConnectionStringCredentials()
    dict_explicit = {"explicit": "is_dict"}
    config_explicit = BaseConfiguration()
    assert resolve._maybe_parse_native_value(c, dict_explicit, ()) is dict_explicit
    assert resolve._maybe_parse_native_value(c, config_explicit, ()) is config_explicit

    # postgres credentials have a default parameter (connect_timeout), which must be removed for explicit value
    pg_c = PostgresCredentials()
    explicit_value = resolve._maybe_parse_native_value(
        pg_c, "postgres://loader@localhost:5432/dlt_data?a=b&c=d", ()
    )
    # NOTE: connect_timeout and password are not present
    assert explicit_value == {
        "drivername": "postgres",
        "database": "dlt_data",
        "username": "loader",
        "host": "localhost",
        "query": {"a": "b", "c": "d"},
    }
    pg_c = PostgresCredentials()
    explicit_value = resolve._maybe_parse_native_value(
        pg_c, "postgres://loader@localhost:5432/dlt_data?connect_timeout=33", ()
    )
    assert explicit_value["connect_timeout"] == 33


def test_optional_params_resolved_if_complete_native_value(environment: Any) -> None:
    # this native value fully resolves configuration
    environment["CREDENTIALS"] = "postgres://loader:pwd@localhost:5432/dlt_data?a=b&c=d"
    # still this config value will be injected
    environment["CREDENTIALS__CONNECT_TIMEOUT"] = "300"
    c = resolve.resolve_configuration(PostgresCredentials())
    assert c.connect_timeout == 300


def test_on_resolved(environment: Any) -> None:
    with pytest.raises(RuntimeError):
        # head over hells
        resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value="he>a>b>h")


def test_embedded_config(environment: Any) -> None:
    # resolve all embedded config, using explicit value for instrumented config and explicit dict for sectioned config
    C = resolve.resolve_configuration(
        EmbeddedConfiguration(),
        explicit_value={
            "default": "set",
            "instrumented": "h>tu>be>xhe",
            "sectioned": {"password": "pwd"},
        },
    )
    assert C.default == "set"
    assert C.instrumented.to_native_representation() == "h>tu>be>xhe"
    assert C.sectioned.password == "pwd"

    # resolve but providing values via env
    with custom_environ(
        {
            "INSTRUMENTED__HEAD": "h",
            "INSTRUMENTED__TUBE": '["tu", "u", "be"]',
            "INSTRUMENTED__HEELS": "xhe",
            "SECTIONED__PASSWORD": "passwd",
            "DEFAULT": "DEF",
        }
    ):
        C = resolve.resolve_configuration(EmbeddedConfiguration())
        assert C.default == "DEF"
        assert C.instrumented.to_native_representation() == "h>tu>u>be>xhe"
        assert C.sectioned.password == "passwd"

    # resolve partial, partial is passed to embedded
    C = resolve.resolve_configuration(EmbeddedConfiguration(), accept_partial=True)
    assert not C.__is_resolved__
    assert not C.sectioned.__is_resolved__
    assert not C.instrumented.__is_resolved__

    # some are partial, some are not
    with custom_environ({"SECTIONED__PASSWORD": "passwd"}):
        C = resolve.resolve_configuration(EmbeddedConfiguration(), accept_partial=True)
        assert not C.__is_resolved__
        assert C.sectioned.__is_resolved__
        assert not C.instrumented.__is_resolved__

    # single integrity error fails all the embeds
    # make the instance sectioned so it can read from INSTRUMENTED
    with patch.object(InstrumentedConfiguration, "__section__", "instrumented"):
        with custom_environ({"INSTRUMENTED": "he>tu>u>be>h"}):
            with pytest.raises(RuntimeError):
                resolve.resolve_configuration(
                    EmbeddedConfiguration(),
                    explicit_value={"default": "set", "sectioned": {"password": "pwd"}},
                )

    # part via env part via explicit values
    with custom_environ(
        {
            "INSTRUMENTED__HEAD": "h",
            "INSTRUMENTED__TUBE": '["tu", "u", "be"]',
            "INSTRUMENTED__HEELS": "xhe",
        }
    ):
        C = resolve.resolve_configuration(
            EmbeddedConfiguration(),
            explicit_value={"default": "set", "sectioned": {"password": "pwd"}},
        )
        assert C.instrumented.to_native_representation() == "h>tu>u>be>xhe"


def test_embedded_explicit_value_over_provider(environment: Any) -> None:
    # make the instance sectioned so it can read from INSTRUMENTED
    with patch.object(InstrumentedConfiguration, "__section__", "instrumented"):
        with custom_environ({"INSTRUMENTED": "h>tu>u>be>he"}):
            # explicit value over the env
            c = resolve.resolve_configuration(
                EmbeddedConfiguration(),
                explicit_value={"instrumented": "h>tu>be>xhe"},
                accept_partial=True,
            )
            assert c.instrumented.to_native_representation() == "h>tu>be>xhe"
            # parent configuration is not resolved
            assert not c.is_resolved()
            assert c.is_partial()
            # but embedded is
            assert c.instrumented.__is_resolved__
            assert c.instrumented.is_resolved()
            assert not c.instrumented.is_partial()


def test_provider_values_over_embedded_default(environment: Any) -> None:
    # make the instance sectioned so it can read from INSTRUMENTED
    with patch.object(InstrumentedConfiguration, "__section__", "instrumented"):
        with custom_environ({"INSTRUMENTED": "h>tu>u>be>he"}):
            # read from env - over the default values
            InstrumentedConfiguration().parse_native_representation("h>tu>be>xhe")
            c = resolve.resolve_configuration(
                EmbeddedConfiguration(instrumented=None), accept_partial=True
            )
            assert c.instrumented.to_native_representation() == "h>tu>u>be>he"
            # parent configuration is not resolved
            assert not c.is_resolved()
            assert c.is_partial()
            # but embedded is
            assert c.instrumented.__is_resolved__
            assert c.instrumented.is_resolved()
            assert not c.instrumented.is_partial()


def test_run_configuration_gen_name(environment: Any) -> None:
    C = resolve.resolve_configuration(RuntimeConfiguration())
    assert C.pipeline_name.startswith("dlt_")


def test_configuration_is_mutable_mapping(environment: Any, env_provider: ConfigProvider) -> None:
    @configspec
    class _SecretCredentials(RuntimeConfiguration):
        pipeline_name: Optional[str] = "secret"
        secret_value: TSecretValue = None
        config_files_storage_path: str = "storage"

    # configurations provide full MutableMapping support
    # here order of items in dict matters
    expected_dict = {
        "pipeline_name": "secret",
        "sentry_dsn": None,
        "slack_incoming_hook": None,
        "dlthub_telemetry": True,
        "dlthub_telemetry_endpoint": "https://telemetry-tracker.services4758.workers.dev",
        "dlthub_telemetry_segment_write_key": None,
        "log_format": "{asctime}|[{levelname}]|{process}|{thread}|{name}|{filename}|{funcName}:{lineno}|{message}",
        "log_level": "WARNING",
        "request_timeout": 60,
        "request_max_attempts": 5,
        "request_backoff_factor": 1,
        "request_max_retry_delay": 300,
        "config_files_storage_path": "storage",
        "dlthub_dsn": None,
        "secret_value": None,
    }
    assert dict(_SecretCredentials()) == expected_dict

    environment["RUNTIME__SECRET_VALUE"] = "secret"
    # get_resolved_traces().clear()
    c = resolve.resolve_configuration(_SecretCredentials())
    # print(get_resolved_traces())
    expected_dict["secret_value"] = "secret"
    assert dict(c) == expected_dict

    # check mutable mapping type
    assert isinstance(c, MutableMapping)
    assert isinstance(c, Mapping)
    assert not isinstance(c, Dict)

    # check view ops
    assert c.keys() == expected_dict.keys()
    assert len(c) == len(expected_dict)
    assert c.items() == expected_dict.items()
    # comparing list compares order
    assert list(c.values()) == list(expected_dict.values())
    for key in c:
        assert c[key] == expected_dict[key]
    # version is present as attr but not present in dict
    assert hasattr(c, "__is_resolved__")
    assert hasattr(c, "__section__")

    # set ops
    # update supported and non existing attributes are ignored
    c.update({"pipeline_name": "old pipe", "__version": "1.1.1"})
    assert c.pipeline_name == "old pipe" == c["pipeline_name"]

    # delete is not supported
    with pytest.raises(KeyError):
        del c["pipeline_name"]

    with pytest.raises(KeyError):
        c.pop("pipeline_name", None)

    # setting supported
    c["pipeline_name"] = "new pipe"
    assert c.pipeline_name == "new pipe" == c["pipeline_name"]
    with pytest.raises(KeyError):
        c["unknown_prop"] = "unk"

    # also on new instance
    c2 = SecretConfiguration()
    with pytest.raises(KeyError):
        c2["unknown_prop"] = "unk"


def test_fields_with_no_default_to_null(environment: Any) -> None:
    # fields with no default are promoted to class attrs with none
    assert FieldWithNoDefaultConfiguration.no_default is None
    assert FieldWithNoDefaultConfiguration().no_default is None


def test_init_method_gen(environment: Any) -> None:
    C = FieldWithNoDefaultConfiguration(no_default="no_default", sentry_dsn="SENTRY")
    assert C.no_default == "no_default"
    assert C.sentry_dsn == "SENTRY"


def test_multi_derivation_defaults(environment: Any) -> None:
    @configspec
    class MultiConfiguration(
        SectionedConfiguration, MockProdConfiguration, ConfigurationWithOptionalTypes
    ):
        pass

    # apparently dataclasses set default in reverse mro so MockProdConfiguration overwrites
    C = MultiConfiguration()
    assert C.pipeline_name == MultiConfiguration.pipeline_name == "comp"
    # but keys are ordered in MRO so password from ConfigurationWithOptionalTypes goes first
    keys = list(C.keys())
    assert keys[0] == "pipeline_name"
    # SectionedConfiguration last field goes last
    assert keys[-1] == "password"

    # section from SectionedConfiguration prevails
    assert C.__section__ == "DLT_TEST"


def test_raises_on_unresolved_field(environment: Any, env_provider: ConfigProvider) -> None:
    # via make configuration
    with pytest.raises(ConfigFieldMissingException) as cf_missing_exc:
        resolve.resolve_configuration(WrongConfiguration())
    assert cf_missing_exc.value.spec_name == "WrongConfiguration"
    assert "NoneConfigVar" in cf_missing_exc.value.traces
    # has only one trace
    trace = cf_missing_exc.value.traces["NoneConfigVar"]
    assert len(trace) == 1
    assert trace[0] == LookupTrace("Environment Variables", [], "NONECONFIGVAR", None)
    # toml providers were empty and are not returned in trace
    # assert trace[1] == LookupTrace("secrets.toml", [], "NoneConfigVar", None)
    # assert trace[2] == LookupTrace("config.toml", [], "NoneConfigVar", None)
    # check the exception trace
    exception_traces = get_exception_trace_chain(cf_missing_exc.value)
    assert len(exception_traces) == 1
    exception_trace = exception_traces[0]
    assert exception_trace["docstring"] == ConfigFieldMissingException.__doc__
    # serialized traces
    assert "NoneConfigVar" in exception_trace["exception_attrs"]["traces"]
    assert exception_trace["exception_attrs"]["spec_name"] == "WrongConfiguration"
    assert exception_trace["exception_attrs"]["fields"] == ["NoneConfigVar"]


def test_raises_on_many_unresolved_fields(environment: Any, env_provider: ConfigProvider) -> None:
    # via make configuration
    with pytest.raises(ConfigFieldMissingException) as cf_missing_exc:
        resolve.resolve_configuration(CoercionTestConfiguration())
    # check the exception trace
    exception_trace = get_exception_trace(cf_missing_exc.value)

    assert cf_missing_exc.value.spec_name == "CoercionTestConfiguration"
    # get all fields that must be set
    val_fields = [
        f for f in CoercionTestConfiguration().get_resolvable_fields() if f.lower().endswith("_val")
    ]
    traces = cf_missing_exc.value.traces
    assert len(traces) == len(val_fields)
    for tr_field, exp_field in zip(traces, val_fields):
        assert len(traces[tr_field]) == 1
        assert traces[tr_field][0] == LookupTrace(
            "Environment Variables",
            [],
            environ_provider.EnvironProvider.get_key_name(exp_field),
            None,
        )
        # field must be in exception trace
        assert tr_field in exception_trace["exception_attrs"]["fields"]
        assert tr_field in exception_trace["exception_attrs"]["traces"]
        # assert traces[tr_field][1] == LookupTrace("secrets.toml", [], toml.TomlFileProvider.get_key_name(exp_field), None)
        # assert traces[tr_field][2] == LookupTrace("config.toml", [], toml.TomlFileProvider.get_key_name(exp_field), None)


def test_removes_trace_value_from_exception_trace_attrs(
    environment: Any, env_provider: ConfigProvider
) -> None:
    with pytest.raises(ConfigFieldMissingException) as cf_missing_exc:
        resolve.resolve_configuration(CoercionTestConfiguration())
    cf_missing_exc.value.traces["str_val"][0] = cf_missing_exc.value.traces["str_val"][0]._replace(value="SECRET")  # type: ignore[index]
    assert cf_missing_exc.value.traces["str_val"][0].value == "SECRET"
    attrs_ = cf_missing_exc.value.attrs()
    # values got cleared up
    assert attrs_["traces"]["str_val"][0].value is None


def test_accepts_optional_missing_fields(environment: Any) -> None:
    # ConfigurationWithOptionalTypes has values for all non optional fields present
    C = ConfigurationWithOptionalTypes()
    assert not C.is_partial()
    # make optional config
    resolve.resolve_configuration(ConfigurationWithOptionalTypes())
    # make config with optional values
    resolve.resolve_configuration(
        ProdConfigurationWithOptionalTypes(), explicit_value={"int_val": None}
    )
    # make config with optional embedded config
    C2 = resolve.resolve_configuration(EmbeddedOptionalConfiguration())
    # embedded config was not fully resolved
    assert C2.instrumented is None


def test_find_all_keys() -> None:
    keys = VeryWrongConfiguration().get_resolvable_fields()
    # assert hints and types: LOG_COLOR had it hint overwritten in derived class
    assert set(
        {"str_val": str, "int_val": int, "NoneConfigVar": str, "log_color": str}.items()
    ).issubset(keys.items())


def test_coercion_to_hint_types(environment: Any) -> None:
    add_config_dict_to_env(COERCIONS, destructure_dicts=False)

    C = CoercionTestConfiguration()
    resolve._resolve_config_fields(
        C, explicit_values=None, explicit_sections=(), embedded_sections=(), accept_partial=False
    )

    for key in COERCIONS:
        assert getattr(C, key) == COERCIONS[key]


def test_values_serialization() -> None:
    # test tuple
    t_tuple = (1, 2, 3, "A")
    v = serialize_value(t_tuple)
    assert v == "(1, 2, 3, 'A')"  # literal serialization
    assert deserialize_value("K", v, tuple) == t_tuple

    # test list
    t_list = ["a", 3, True]
    v = serialize_value(t_list)
    assert v == '["a",3,true]'  # json serialization
    assert deserialize_value("K", v, list) == t_list

    # test datetime
    t_date = pendulum.now()
    v = serialize_value(t_date)
    assert deserialize_value("K", v, datetime.datetime) == t_date

    # test wei
    t_wei = Wei.from_int256(10**16, decimals=18)
    v = serialize_value(t_wei)
    assert v == "0.01"
    # can be deserialized into
    assert deserialize_value("K", v, float) == 0.01
    assert deserialize_value("K", v, Decimal) == Decimal("0.01")
    assert deserialize_value("K", v, Wei) == Wei("0.01")

    # test credentials
    credentials_str = "databricks+connector://token:-databricks_token-@<databricks_host>:443/<database_or_schema_name>?conn_timeout=15&search_path=a%2Cb%2Cc"
    credentials = deserialize_value("credentials", credentials_str, ConnectionStringCredentials)
    assert credentials.drivername == "databricks+connector"
    assert credentials.query == {"conn_timeout": "15", "search_path": "a,b,c"}
    assert credentials.password == "-databricks_token-"
    assert serialize_value(credentials) == credentials_str
    # using dict also works
    credentials_dict = dict(credentials)
    credentials_2 = deserialize_value("credentials", credentials_dict, ConnectionStringCredentials)
    assert serialize_value(credentials_2) == credentials_str
    # if string is not a valid native representation of credentials but is parsable json dict then it works as well
    credentials_json = json.dumps(credentials_dict)
    credentials_3 = deserialize_value("credentials", credentials_json, ConnectionStringCredentials)
    assert serialize_value(credentials_3) == credentials_str

    # test config without native representation
    secret_config = deserialize_value("credentials", {"secret_value": "a"}, SecretConfiguration)
    assert secret_config.secret_value == "a"
    secret_config = deserialize_value("credentials", '{"secret_value": "a"}', SecretConfiguration)
    assert secret_config.secret_value == "a"
    assert serialize_value(secret_config) == '{"secret_value":"a"}'


def test_invalid_coercions(environment: Any) -> None:
    C = CoercionTestConfiguration()
    add_config_dict_to_env(INVALID_COERCIONS, destructure_dicts=False)
    for key, value in INVALID_COERCIONS.items():
        try:
            resolve._resolve_config_fields(
                C,
                explicit_values=None,
                explicit_sections=(),
                embedded_sections=(),
                accept_partial=False,
            )
        except ConfigValueCannotBeCoercedException as coerc_exc:
            # must fail exactly on expected value
            if coerc_exc.field_name != key:
                raise
            # overwrite with valid value and go to next env
            environment[key.upper()] = serialize_value(COERCIONS[key])
            continue
        raise AssertionError("%s was coerced with %s which is invalid type" % (key, value))


def test_excepted_coercions(environment: Any) -> None:
    C = CoercionTestConfiguration()
    add_config_dict_to_env(COERCIONS, destructure_dicts=False)
    add_config_dict_to_env(EXCEPTED_COERCIONS, overwrite_keys=True, destructure_dicts=False)
    resolve._resolve_config_fields(
        C, explicit_values=None, explicit_sections=(), embedded_sections=(), accept_partial=False
    )
    for key in EXCEPTED_COERCIONS:
        assert getattr(C, key) == COERCED_EXCEPTIONS[key]


def test_config_with_unsupported_types_in_hints(environment: Any) -> None:
    with pytest.raises(ConfigFieldTypeHintNotSupported):

        @configspec
        class InvalidHintConfiguration(BaseConfiguration):
            tuple_val: tuple = None  # type: ignore
            set_val: set = None  # type: ignore

        InvalidHintConfiguration()


def test_config_with_no_hints(environment: Any) -> None:
    with pytest.raises(ConfigFieldMissingTypeHintException):

        @configspec
        class NoHintConfiguration(BaseConfiguration):
            tuple_val = None

        NoHintConfiguration()


def test_config_with_non_templated_nested_hints(environment: Any) -> None:
    environment["LIST_VAL"] = "[1,2,3]"
    environment["TUPLE_VAL"] = "(1,2,3)"
    environment["DICT_VAL"] = '{"a": 1}'
    c = resolve.resolve_configuration(NonTemplatedNestedTypesConfiguration())
    assert c.list_val == [1, 2, 3]
    assert c.tuple_val == (1, 2, 3)
    assert c.dict_val == {"a": 1}


def test_resolve_configuration(environment: Any) -> None:
    # fill up configuration
    environment["NONECONFIGVAR"] = "1"
    C = resolve.resolve_configuration(WrongConfiguration())
    assert C.__is_resolved__
    assert C.NoneConfigVar == "1"


def test_dataclass_instantiation(environment: Any) -> None:
    # resolve_configuration works on instances of dataclasses and types are not modified
    environment["SECRET_VALUE"] = "1"
    C = resolve.resolve_configuration(SecretConfiguration())
    # auto derived type holds the value
    assert C.secret_value == "1"
    # base type is untouched
    assert SecretConfiguration.secret_value is None


def test_accept_partial(environment: Any) -> None:
    # modify original type
    WrongConfiguration.NoneConfigVar = None
    # that None value will be present in the instance
    C = resolve.resolve_configuration(WrongConfiguration(), accept_partial=True)
    assert C.NoneConfigVar is None
    # partial resolution
    assert not C.__is_resolved__
    assert C.is_partial()


def test_coercion_rules() -> None:
    with pytest.raises(ConfigValueCannotBeCoercedException):
        coerce_single_value("key", "some string", int)
    assert coerce_single_value("key", "some string", str) == "some string"
    # Optional[str] has type object, mypy will never work properly...
    assert coerce_single_value("key", "some string", Optional[str]) == "some string"  # type: ignore

    assert coerce_single_value("key", "234", int) == 234
    assert coerce_single_value("key", "234", Optional[int]) == 234  # type: ignore

    # check coercions of NewTypes
    assert coerce_single_value("key", "test str X", FirstOrderStr) == "test str X"
    assert coerce_single_value("key", "test str X", Optional[FirstOrderStr]) == "test str X"  # type: ignore
    assert coerce_single_value("key", "test str X", Optional[SecondOrderStr]) == "test str X"  # type: ignore
    assert coerce_single_value("key", "test str X", SecondOrderStr) == "test str X"
    assert coerce_single_value("key", "234", LongInteger) == 234
    assert coerce_single_value("key", "234", Optional[LongInteger]) == 234  # type: ignore
    # this coercion should fail
    with pytest.raises(ConfigValueCannotBeCoercedException):
        coerce_single_value("key", "some string", LongInteger)
    with pytest.raises(ConfigValueCannotBeCoercedException):
        coerce_single_value("key", "some string", Optional[LongInteger])  # type: ignore


def test_is_valid_hint() -> None:
    assert is_valid_hint(Any) is True  # type: ignore[arg-type]
    assert is_valid_hint(Optional[Any]) is True  # type: ignore[arg-type]
    assert is_valid_hint(RuntimeConfiguration) is True
    assert is_valid_hint(Optional[RuntimeConfiguration]) is True  # type: ignore[arg-type]
    assert is_valid_hint(TSecretValue) is True
    assert is_valid_hint(Optional[TSecretValue]) is True  # type: ignore[arg-type]
    # in case of generics, origin will be used and args are not checked
    assert is_valid_hint(MutableMapping[TSecretValue, Any]) is True
    # this is valid (args not checked)
    assert is_valid_hint(MutableMapping[TSecretValue, ConfigValueCannotBeCoercedException]) is True
    assert is_valid_hint(Wei) is True
    # any class type, except deriving from BaseConfiguration is wrong type
    assert is_valid_hint(ConfigFieldMissingException) is False
    # but final and annotated types are not ok because they are not resolved
    assert is_valid_hint(Final[ConfigFieldMissingException]) is True  # type: ignore[arg-type]
    assert is_valid_hint(Annotated[ConfigFieldMissingException, NotResolved()]) is True  # type: ignore[arg-type]
    assert is_valid_hint(Annotated[ConfigFieldMissingException, "REQ"]) is False  # type: ignore[arg-type]


def test_is_not_resolved_hint() -> None:
    assert is_hint_not_resolvable(Final[ConfigFieldMissingException]) is True
    assert is_hint_not_resolvable(Annotated[ConfigFieldMissingException, NotResolved()]) is True
    assert is_hint_not_resolvable(Annotated[ConfigFieldMissingException, NotResolved(True)]) is True
    assert (
        is_hint_not_resolvable(Annotated[ConfigFieldMissingException, NotResolved(False)]) is False
    )
    assert is_hint_not_resolvable(Annotated[ConfigFieldMissingException, "REQ"]) is False
    assert is_hint_not_resolvable(str) is False


def test_not_resolved_hint() -> None:
    class SentinelClass:
        pass

    @configspec
    class OptionalNotResolveConfiguration(BaseConfiguration):
        trace: Final[Optional[SentinelClass]] = None
        traces: Annotated[Optional[List[SentinelClass]], NotResolved()] = None

    c = resolve.resolve_configuration(OptionalNotResolveConfiguration())
    assert c.trace is None
    assert c.traces is None

    s1 = SentinelClass()
    s2 = SentinelClass()

    c = resolve.resolve_configuration(OptionalNotResolveConfiguration(s1, [s2]))
    assert c.trace is s1
    assert c.traces[0] is s2

    @configspec
    class NotResolveConfiguration(BaseConfiguration):
        trace: Final[SentinelClass] = None
        traces: Annotated[List[SentinelClass], NotResolved()] = None

    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(NotResolveConfiguration())

    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(NotResolveConfiguration(trace=s1))

    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(NotResolveConfiguration(traces=[s2]))

    c2 = resolve.resolve_configuration(NotResolveConfiguration(s1, [s2]))
    assert c2.trace is s1
    assert c2.traces[0] is s2


def test_configspec_auto_base_config_derivation() -> None:
    @configspec
    class AutoBaseDerivationConfiguration:
        auto: str = None

    assert issubclass(AutoBaseDerivationConfiguration, BaseConfiguration)
    assert hasattr(AutoBaseDerivationConfiguration, "auto")

    assert AutoBaseDerivationConfiguration().auto is None
    assert AutoBaseDerivationConfiguration(auto="auto").auto == "auto"
    assert AutoBaseDerivationConfiguration(auto="auto").get_resolvable_fields() == {"auto": str}  # type: ignore[attr-defined]
    # we preserve original module
    assert AutoBaseDerivationConfiguration.__module__ == __name__
    assert not hasattr(BaseConfiguration, "auto")


def test_secret_value_not_secret_provider(mock_provider: MockProvider) -> None:
    mock_provider.value = "SECRET"

    # TSecretValue will fail
    with pytest.raises(ValueNotSecretException) as py_ex:
        resolve.resolve_configuration(SecretConfiguration(), sections=("mock",))
    assert py_ex.value.provider_name == "Mock Provider"
    assert py_ex.value.key == "-secret_value"

    # anything derived from CredentialsConfiguration will fail
    with patch.object(SecretCredentials, "__section__", "credentials"):
        with pytest.raises(ValueNotSecretException) as py_ex:
            resolve.resolve_configuration(WithCredentialsConfiguration(), sections=("mock",))
        assert py_ex.value.provider_name == "Mock Provider"
        assert py_ex.value.key == "-credentials"


def test_do_not_resolve_twice(environment: Any) -> None:
    environment["SECRET_VALUE"] = "password"
    c = resolve.resolve_configuration(SecretConfiguration())
    assert c.secret_value == "password"
    c2 = SecretConfiguration()
    c2.secret_value = "other"
    c2.__is_resolved__ = True
    assert c2.is_resolved()
    # will not overwrite with env
    c3 = resolve.resolve_configuration(c2)
    assert c3.secret_value == "other"
    assert c3 is c2
    # make it not resolved
    c2.__is_resolved__ = False
    c4 = resolve.resolve_configuration(c2)
    assert c4.secret_value == "password"
    assert c2 is c3 is c4
    # also c is resolved so
    c.secret_value = "else"
    assert resolve.resolve_configuration(c).secret_value == "else"


def test_do_not_resolve_embedded(environment: Any) -> None:
    environment["SECRET__SECRET_VALUE"] = "password"
    c = resolve.resolve_configuration(EmbeddedSecretConfiguration())
    assert c.secret.secret_value == "password"
    c2 = SecretConfiguration()
    c2.secret_value = "other"
    c2.__is_resolved__ = True
    embed_c = EmbeddedSecretConfiguration()
    embed_c.secret = c2
    embed_c2 = resolve.resolve_configuration(embed_c)
    assert embed_c2.secret.secret_value == "other"
    assert embed_c2.secret is c2


def test_last_resolve_exception(environment: Any) -> None:
    # partial will set the ConfigEntryMissingException
    c = resolve.resolve_configuration(EmbeddedConfiguration(), accept_partial=True)
    assert isinstance(c.__exception__, ConfigFieldMissingException)
    # missing keys
    c2 = SecretConfiguration()
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(c2)
    assert c2.__exception__ is py_ex.value
    # but if ran again exception is cleared
    environment["SECRET_VALUE"] = "password"
    resolve.resolve_configuration(c2)
    assert c2.__exception__ is None
    # explicit value
    c3 = InstrumentedConfiguration()
    with pytest.raises(InvalidNativeValue) as py_ex2:
        resolve.resolve_configuration(c3, explicit_value=2137)
    assert c3.__exception__ is py_ex2.value


def test_resolved_trace(environment: Any) -> None:
    with custom_environ(
        {
            "INSTRUMENTED__HEAD": "h",
            "INSTRUMENTED__TUBE": '["tu", "u", "be"]',
            "INSTRUMENTED__HEELS": "xhe",
            "SECTIONED__PASSWORD": "passwd",
            "DEFAULT": "DEF",
        }
    ):
        c = resolve.resolve_configuration(EmbeddedConfiguration(default="_DEFF"))
    traces = get_resolved_traces()
    prov_name = environ_provider.EnvironProvider().name
    assert traces[".default"] == ResolvedValueTrace(
        "default", "DEF", "_DEFF", str, [], prov_name, c
    )
    assert traces["instrumented.head"] == ResolvedValueTrace(
        "head", "h", None, str, ["instrumented"], prov_name, c.instrumented
    )
    # value is before casting
    assert traces["instrumented.tube"] == ResolvedValueTrace(
        "tube", '["tu", "u", "be"]', None, List[str], ["instrumented"], prov_name, c.instrumented
    )
    assert deserialize_value(
        "tube", traces["instrumented.tube"].value, resolve.extract_inner_hint(List[str])
    ) == ["tu", "u", "be"]
    assert traces["instrumented.heels"] == ResolvedValueTrace(
        "heels", "xhe", None, str, ["instrumented"], prov_name, c.instrumented
    )
    assert traces["sectioned.password"] == ResolvedValueTrace(
        "password", "passwd", None, str, ["sectioned"], prov_name, c.sectioned
    )
    assert len(traces) == 5

    # try to get native representation
    with patch.object(InstrumentedConfiguration, "__section__", "snake"):
        with custom_environ(
            {
                "INSTRUMENTED": "h>t>t>t>he",
                "SECTIONED__PASSWORD": "pass",
                "DEFAULT": "UNDEF",
                "SNAKE": "h>t>t>t>he",
            }
        ):
            c = resolve.resolve_configuration(EmbeddedConfiguration())
            resolve.resolve_configuration(InstrumentedConfiguration())

    assert traces[".default"] == ResolvedValueTrace("default", "UNDEF", None, str, [], prov_name, c)
    assert traces[".instrumented"] == ResolvedValueTrace(
        "instrumented", "h>t>t>t>he", None, InstrumentedConfiguration, [], prov_name, c
    )

    assert traces[".snake"] == ResolvedValueTrace(
        "snake", "h>t>t>t>he", None, InstrumentedConfiguration, [], prov_name, None
    )


def test_extract_inner_hint() -> None:
    # extracts base config from an union
    assert resolve.extract_inner_hint(Union[GcpServiceAccountCredentialsWithoutDefaults, StrAny, str]) is GcpServiceAccountCredentialsWithoutDefaults  # type: ignore[arg-type]
    assert resolve.extract_inner_hint(Union[InstrumentedConfiguration, StrAny, str]) is InstrumentedConfiguration  # type: ignore[arg-type]
    # keeps unions
    assert resolve.extract_inner_hint(Union[StrAny, str]) is Union  # type: ignore[arg-type]
    # ignores specialization in list and dict, leaving origin
    assert resolve.extract_inner_hint(List[str]) is list
    assert resolve.extract_inner_hint(DictStrAny) is dict
    # extracts new types
    assert resolve.extract_inner_hint(TSecretValue) is AnyType
    # preserves new types on extract
    assert resolve.extract_inner_hint(CallableAny, preserve_new_types=True) is CallableAny
    # extracts and preserves annotated
    assert resolve.extract_inner_hint(Optional[Annotated[int, "X"]]) is int  # type: ignore[arg-type]
    TAnnoInt = Annotated[int, "X"]
    assert resolve.extract_inner_hint(Optional[TAnnoInt], preserve_annotated=True) is TAnnoInt  # type: ignore[arg-type]
    # extracts and preserves literals
    TLit = Literal["a", "b"]
    TAnnoLit = Annotated[TLit, "X"]
    assert resolve.extract_inner_hint(TAnnoLit, preserve_literal=True) is TLit  # type: ignore[arg-type]
    assert resolve.extract_inner_hint(TAnnoLit, preserve_literal=False) is str  # type: ignore[arg-type]


def test_is_secret_hint() -> None:
    assert resolve.is_secret_hint(GcpServiceAccountCredentialsWithoutDefaults) is True
    assert resolve.is_secret_hint(Optional[GcpServiceAccountCredentialsWithoutDefaults]) is True  # type: ignore[arg-type]
    assert resolve.is_secret_hint(TSecretValue) is True
    assert resolve.is_secret_hint(TSecretStrValue) is True
    assert resolve.is_secret_hint(Optional[TSecretValue]) is True  # type: ignore[arg-type]
    assert resolve.is_secret_hint(InstrumentedConfiguration) is False
    # do not recognize new types
    TTestSecretNt = NewType("TTestSecretNt", GcpServiceAccountCredentialsWithoutDefaults)
    assert resolve.is_secret_hint(TTestSecretNt) is False
    # recognize unions with credentials
    assert resolve.is_secret_hint(Union[GcpServiceAccountCredentialsWithoutDefaults, StrAny, str]) is True  # type: ignore[arg-type]
    # we do not recognize unions if they do not contain configuration types
    assert resolve.is_secret_hint(Union[TSecretValue, StrAny, str]) is False  # type: ignore[arg-type]
    assert resolve.is_secret_hint(Optional[str]) is False  # type: ignore[arg-type]
    assert resolve.is_secret_hint(str) is False
    assert resolve.is_secret_hint(AnyType) is False


def test_is_secret_hint_custom_type() -> None:
    # any type annotated with SecretSentinel is secret
    assert resolve.is_secret_hint(Annotated[int, SecretSentinel]) is True  # type: ignore[arg-type]


def coerce_single_value(key: str, value: str, hint: Type[Any]) -> Any:
    hint = extract_inner_type(hint)
    return resolve.deserialize_value(key, value, hint)


def test_dynamic_type_hint(environment: Dict[str, str]) -> None:
    """Test dynamic type hint using @resolve_type decorator"""
    environment["DUMMY__DISCRIMINATOR"] = "b"
    environment["DUMMY__EMBEDDED_CONFIG__FIELD_FOR_B"] = "some_value"

    config = resolve.resolve_configuration(ConfigWithDynamicType(), sections=("dummy",))

    assert isinstance(config.embedded_config, DynamicConfigB)
    assert config.embedded_config.field_for_b == "some_value"


def test_dynamic_type_hint_subclass(environment: Dict[str, str]) -> None:
    """Test overriding @resolve_type method in subclass"""
    environment["DUMMY__IS_NUMBER"] = "true"
    environment["DUMMY__DYNAMIC_TYPE_FIELD"] = "22"

    # Test extended resolver method is applied
    environment["DUMMY__DISCRIMINATOR"] = "c"
    environment["DUMMY__EMBEDDED_CONFIG__FIELD_FOR_C"] = "some_value"

    config = resolve.resolve_configuration(SubclassConfigWithDynamicType(), sections=("dummy",))

    assert isinstance(config.embedded_config, DynamicConfigC)
    assert config.embedded_config.field_for_c == "some_value"

    # Test super() call is applied correctly
    environment["DUMMY__DISCRIMINATOR"] = "b"
    environment["DUMMY__EMBEDDED_CONFIG__FIELD_FOR_B"] = "some_value"

    config = resolve.resolve_configuration(SubclassConfigWithDynamicType(), sections=("dummy",))

    assert isinstance(config.embedded_config, DynamicConfigB)
    assert config.embedded_config.field_for_b == "some_value"

    # Test second dynamic field added in subclass
    environment["DUMMY__IS_NUMBER"] = "true"
    environment["DUMMY__DYNAMIC_TYPE_FIELD"] = "some"

    with pytest.raises(ConfigValueCannotBeCoercedException) as e:
        config = resolve.resolve_configuration(SubclassConfigWithDynamicType(), sections=("dummy",))

    assert e.value.field_name == "dynamic_type_field"
    assert e.value.hint == int


def test_unmatched_dynamic_hint_resolvers(environment: Dict[str, str]) -> None:
    with pytest.raises(UnmatchedConfigHintResolversException) as e:
        resolve.resolve_configuration(ConfigWithInvalidDynamicType())

    print(e.value)

    assert set(e.value.field_names) == {"a", "b", "c"}
    assert e.value.spec_name == ConfigWithInvalidDynamicType.__name__


def test_add_config_to_env(environment: Dict[str, str]) -> None:
    c = resolve.resolve_configuration(
        EmbeddedConfiguration(
            instrumented="h>tu>u>be>he",  # type: ignore[arg-type]
            sectioned=SectionedConfiguration(password="PASS"),
            default="BUBA",
        )
    )
    add_config_to_env(c, ("dlt",))
    # must contain dlt prefix everywhere, INSTRUMENTED section taken from key and DLT_TEST taken from password
    assert (
        environment.items()
        >= {
            "DLT__DEFAULT": "BUBA",
            "DLT__INSTRUMENTED__HEAD": "h",
            "DLT__INSTRUMENTED__TUBE": '["tu","u","be"]',
            "DLT__INSTRUMENTED__HEELS": "he",
            "DLT__DLT_TEST__PASSWORD": "PASS",
        }.items()
    )
    # no dlt
    environment.clear()
    add_config_to_env(c)
    assert (
        environment.items()
        == {
            "DEFAULT": "BUBA",
            "INSTRUMENTED__HEAD": "h",
            "INSTRUMENTED__TUBE": '["tu","u","be"]',
            "INSTRUMENTED__HEELS": "he",
            "DLT_TEST__PASSWORD": "PASS",
        }.items()
    )
    # starts with sectioned
    environment.clear()
    add_config_to_env(c.sectioned)
    assert environment == {"DLT_TEST__PASSWORD": "PASS"}

    # dicts should be added as sections
    environment.clear()
    c_s = ConnectionStringCredentials(
        "mssql://loader:<password>@loader.database.windows.net/dlt_data?TrustServerCertificate=yes&Encrypt=yes&LongAsMax=yes"
    )
    add_config_to_env(c_s, ("dlt",))
    assert environment["DLT__CREDENTIALS__QUERY__ENCRYPT"] == "yes"
    assert environment["DLT__CREDENTIALS__QUERY__TRUSTSERVERCERTIFICATE"] == "yes"


def test_configuration_copy() -> None:
    c = resolve.resolve_configuration(
        EmbeddedConfiguration(),
        explicit_value={
            "default": "set",
            "instrumented": "h>tu>be>xhe",
            "sectioned": {"password": "pwd"},
        },
    )
    assert c.is_resolved()
    copy_c = c.copy()
    assert copy_c.is_resolved()
    assert c.default == copy_c.default
    assert c.instrumented is not copy_c.instrumented
    assert dict(c.instrumented) == dict(copy_c.instrumented)

    # try credentials
    cred = ConnectionStringCredentials()
    cred.parse_native_representation("postgresql://loader:loader@localhost:5432/dlt_data")
    copy_cred = cred.copy()
    assert dict(copy_cred) == dict(cred)
    assert (
        copy_cred.to_native_representation() == "postgresql://loader:loader@localhost:5432/dlt_data"
    )
    # resolve the copy
    assert not copy_cred.is_resolved()
    resolved_cred_copy = c = resolve.resolve_configuration(copy_cred)  # type: ignore[assignment]
    assert resolved_cred_copy.is_resolved()


def test_configuration_with_configuration_as_default() -> None:
    instrumented_default = InstrumentedConfiguration()
    instrumented_default.parse_native_representation("h>a>b>he")
    cred = ConnectionStringCredentials()
    cred.parse_native_representation("postgresql://loader:loader@localhost:5432/dlt_data")

    @configspec
    class EmbeddedConfigurationWithDefaults(BaseConfiguration):
        default: str = "STR"
        instrumented: InstrumentedConfiguration = instrumented_default
        sectioned: SectionedConfiguration = SectionedConfiguration(password="P")
        conn_str: ConnectionStringCredentials = cred

    c_instance = EmbeddedConfigurationWithDefaults()
    assert c_instance.default == EmbeddedConfigurationWithDefaults.default
    assert c_instance.instrumented is not instrumented_default
    assert dict(c_instance.instrumented) == dict(instrumented_default)
    assert not c_instance.conn_str.is_resolved()
    assert c_instance.conn_str.host == "localhost"
    assert not c_instance.is_resolved()

    c_resolved = resolve.resolve_configuration(c_instance)
    assert c_resolved.is_resolved()
    assert c_resolved.conn_str.is_resolved()


def test_configuration_with_generic(environment: Dict[str, str]) -> None:
    TColumn = TypeVar("TColumn", bound=str)

    @configspec
    class IncrementalConfiguration(BaseConfiguration, Generic[TColumn]):
        # TODO: support generics field
        column: str = ConfigValue

    @configspec
    class SourceConfiguration(BaseConfiguration):
        name: str = ConfigValue
        incremental: IncrementalConfiguration[str] = ConfigValue

    # resolve incremental
    environment["COLUMN"] = "column"
    c = resolve.resolve_configuration(IncrementalConfiguration[str]())
    assert c.column == "column"

    # resolve embedded config with generic
    environment["INCREMENTAL__COLUMN"] = "column_i"
    c2 = resolve.resolve_configuration(SourceConfiguration(name="name"))
    assert c2.incremental.column == "column_i"

    # put incremental in union
    @configspec
    class SourceUnionConfiguration(BaseConfiguration):
        name: str = ConfigValue
        incremental_union: Optional[IncrementalConfiguration[str]] = ConfigValue

    c3 = resolve.resolve_configuration(SourceUnionConfiguration(name="name"))
    assert c3.incremental_union is None
    environment["INCREMENTAL_UNION__COLUMN"] = "column_u"
    c3 = resolve.resolve_configuration(SourceUnionConfiguration(name="name"))
    assert c3.incremental_union.column == "column_u"

    class Sentinel:
        pass

    class SubSentinel(Sentinel):
        pass

    @configspec
    class SourceWideUnionConfiguration(BaseConfiguration):
        name: str = ConfigValue
        incremental_w_union: Union[IncrementalConfiguration[str], str, Sentinel] = ConfigValue
        incremental_sub: Optional[Union[IncrementalConfiguration[str], str, SubSentinel]] = None

    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(SourceWideUnionConfiguration(name="name"))

    # use explicit sentinel
    sentinel = Sentinel()
    c4 = resolve.resolve_configuration(
        SourceWideUnionConfiguration(name="name"), explicit_value={"incremental_w_union": sentinel}
    )
    assert c4.incremental_w_union is sentinel

    # instantiate incremental
    environment["INCREMENTAL_W_UNION__COLUMN"] = "column_w_u"
    c4 = resolve.resolve_configuration(SourceWideUnionConfiguration(name="name"))
    assert c4.incremental_w_union.column == "column_w_u"  # type: ignore[union-attr]

    # sentinel (of super class type) also works for hint of subclass type
    c4 = resolve.resolve_configuration(
        SourceWideUnionConfiguration(name="name"), explicit_value={"incremental_sub": sentinel}
    )
    assert c4.incremental_sub is sentinel


def test_configuration_with_literal_field(environment: Dict[str, str]) -> None:
    """Literal type fields only allow values from the literal"""
    environment["REFRESH"] = "not_a_refresh_mode"

    with pytest.raises(ConfigValueCannotBeCoercedException) as einfo:
        resolve.resolve_configuration(ConfigWithLiteralField())

    assert einfo.value.field_name == "refresh"
    assert einfo.value.field_value == "not_a_refresh_mode"
    assert einfo.value.hint == TRefreshMode

    environment["REFRESH"] = "drop_data"

    spec = resolve.resolve_configuration(ConfigWithLiteralField())
    assert spec.refresh == "drop_data"
