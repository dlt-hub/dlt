import pytest
import datetime  # noqa: I251
from unittest.mock import patch
from typing import Any, Dict, Final, List, Mapping, MutableMapping, NewType, Optional, Sequence, Type, Union, Literal

from dlt.common import json, pendulum, Decimal, Wei
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs.gcp_credentials import GcpServiceAccountCredentialsWithoutDefaults
from dlt.common.utils import custom_environ
from dlt.common.typing import AnyType, DictStrAny, StrAny, TSecretValue, extract_inner_type
from dlt.common.configuration.exceptions import (
    ConfigFieldMissingTypeHintException, ConfigFieldTypeHintNotSupported, FinalConfigFieldException,
    InvalidNativeValue, LookupTrace, ValueNotSecretException, UnmatchedConfigHintResolversException
)
from dlt.common.configuration import configspec, ConfigFieldMissingException, ConfigValueCannotBeCoercedException, resolve, is_valid_hint, resolve_type
from dlt.common.configuration.specs import BaseConfiguration, RunConfiguration, ConnectionStringCredentials
from dlt.common.configuration.providers import environ as environ_provider, toml
from dlt.common.configuration.utils import get_resolved_traces, ResolvedValueTrace, serialize_value, deserialize_value, add_config_dict_to_env

from tests.utils import preserve_environ
from tests.common.configuration.utils import (
    MockProvider, CoercionTestConfiguration, COERCIONS, SecretCredentials, WithCredentialsConfiguration, WrongConfiguration, SecretConfiguration,
    SectionedConfiguration, environment, mock_provider, env_provider, reset_resolved_traces)

INVALID_COERCIONS = {
    # 'STR_VAL': 'test string',  # string always OK
    'int_val': "a12345",
    'bool_val': "not_bool",  # bool overridden by string - that is the most common problem
    'list_val': {"2": 1, "3": 3.0},
    'dict_val': "{'a': 1, 'b', '2'}",
    'bytes_val': 'Hello World!',
    'float_val': "invalid",
    "tuple_val": "{1:2}",
    "date_val": "01 May 2022",
    "dec_val": True
}

EXCEPTED_COERCIONS = {
    # allows to use int for float
    'float_val': 10,
    # allows to use float for str
    'str_val': 10.0
}

COERCED_EXCEPTIONS = {
    # allows to use int for float
    'float_val': 10.0,
    # allows to use float for str
    'str_val': "10.0"
}


@configspec
class VeryWrongConfiguration(WrongConfiguration):
    pipeline_name: str = "Some Name"
    str_val: str = ""
    int_val: int = None
    log_color: str = "1"  # type: ignore


@configspec
class ConfigurationWithOptionalTypes(RunConfiguration):
    pipeline_name: str = "Some Name"

    str_val: Optional[str] = None
    int_val: Optional[int] = None
    bool_val: bool = True


@configspec
class ProdConfigurationWithOptionalTypes(ConfigurationWithOptionalTypes):
    prod_val: str = "prod"


@configspec
class MockProdConfiguration(RunConfiguration):
    pipeline_name: str = "comp"


@configspec(init=True)
class FieldWithNoDefaultConfiguration(RunConfiguration):
    no_default: str


@configspec(init=True)
class InstrumentedConfiguration(BaseConfiguration):
    head: str
    tube: List[str]
    heels: str

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


@configspec(init=True)
class EmbeddedConfiguration(BaseConfiguration):
    default: str
    instrumented: InstrumentedConfiguration
    sectioned: SectionedConfiguration


@configspec
class EmbeddedOptionalConfiguration(BaseConfiguration):
    instrumented: Optional[InstrumentedConfiguration]


@configspec
class EmbeddedSecretConfiguration(BaseConfiguration):
    secret: SecretConfiguration


@configspec
class NonTemplatedComplexTypesConfiguration(BaseConfiguration):
    list_val: list
    tuple_val: tuple
    dict_val: dict


@configspec(init=True)
class DynamicConfigA(BaseConfiguration):
    field_for_a: str


@configspec(init=True)
class DynamicConfigB(BaseConfiguration):
    field_for_b: str


@configspec(init=True)
class DynamicConfigC(BaseConfiguration):
    field_for_c: str


@configspec(init=True)
class ConfigWithDynamicType(BaseConfiguration):
    discriminator: str
    embedded_config: BaseConfiguration

    @resolve_type('embedded_config')
    def resolve_embedded_type(self) -> Type[BaseConfiguration]:
        if self.discriminator == 'a':
            return DynamicConfigA
        elif self.discriminator == 'b':
            return DynamicConfigB
        return BaseConfiguration


@configspec(init=True)
class ConfigWithInvalidDynamicType(BaseConfiguration):
    @resolve_type('a')
    def resolve_a_type(self) -> Type[BaseConfiguration]:
        return DynamicConfigA

    @resolve_type('b')
    def resolve_b_type(self) -> Type[BaseConfiguration]:
        return DynamicConfigB

    @resolve_type('c')
    def resolve_c_type(self) -> Type[BaseConfiguration]:
        return DynamicConfigC


@configspec(init=True)
class SubclassConfigWithDynamicType(ConfigWithDynamicType):
    is_number: bool
    dynamic_type_field: Any

    @resolve_type('embedded_config')
    def resolve_embedded_type(self) -> Type[BaseConfiguration]:
        if self.discriminator == 'c':
            return DynamicConfigC
        return super().resolve_embedded_type()

    @resolve_type('dynamic_type_field')
    def resolve_dynamic_type_field(self) -> Type[Union[int, str]]:
        if self.is_number:
            return int
        return str


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
    c = resolve.resolve_configuration(InstrumentedConfiguration(head="h", tube=["a", "b"], heels="he"))
    assert c.to_native_representation() == "h>a>b>he"
    # set from native form
    c = resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value="h>a>b>he")
    assert c.head == "h"
    assert c.tube == ["a", "b"]
    assert c.heels == "he"
    # set from dictionary
    c = resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value={"head": "h", "tube": ["tu", "be"], "heels": "xhe"})
    assert c.to_native_representation() == "h>tu>be>xhe"


def test_explicit_values(environment: Any) -> None:
    # explicit values override the environment and all else
    environment["PIPELINE_NAME"] = "env name"
    environment["CREATED_VAL"] = "12837"
    # set explicit values and allow partial config
    c = resolve.resolve_configuration(CoercionTestConfiguration(),
        explicit_value={"pipeline_name": "initial name", "none_val": type(environment), "bytes_val": b"str"},
        accept_partial=True
    )
    # explicit
    assert c.pipeline_name == "initial name"
    # explicit (no env)
    assert c.bytes_val == b"str"
    assert c.none_val == type(environment)

    # unknown field in explicit value dict is ignored
    c = resolve.resolve_configuration(CoercionTestConfiguration(), explicit_value={"created_val": "3343"}, accept_partial=True)
    assert "created_val" not in c


def test_explicit_values_false_when_bool() -> None:
    # values like 0, [], "" all coerce to bool False
    c = resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value={"head": "", "tube": [], "heels": ""})
    assert c.head == ""
    assert c.tube == []
    assert c.heels == ""


def test_default_values(environment: Any) -> None:
    # explicit values override the environment and all else
    environment["PIPELINE_NAME"] = "env name"
    environment["CREATED_VAL"] = "12837"
    # set default values and allow partial config
    default = CoercionTestConfiguration()
    default.pipeline_name = "initial name"
    default.none_val = type(environment)
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

    environment["PIPELINE_NAME"] = "comp"
    assert dict(c) == {"pipeline_name": "comp"}
    resolve.resolve_configuration(FinalConfiguration())


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
        c = resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value={"head": "h", "tube": ["tu", "be"], "heels": "uhe"})
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
            resolve.resolve_configuration(EmbeddedConfiguration(), explicit_value={"default": "set", "sectioned": {"password": "pwd"}})
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


def test_on_resolved(environment: Any) -> None:
    with pytest.raises(RuntimeError):
        # head over hells
        resolve.resolve_configuration(InstrumentedConfiguration(), explicit_value="he>a>b>h")


def test_embedded_config(environment: Any) -> None:
    # resolve all embedded config, using explicit value for instrumented config and explicit dict for sectioned config
    C = resolve.resolve_configuration(EmbeddedConfiguration(), explicit_value={"default": "set", "instrumented": "h>tu>be>xhe", "sectioned": {"password": "pwd"}})
    assert C.default == "set"
    assert C.instrumented.to_native_representation() == "h>tu>be>xhe"
    assert C.sectioned.password == "pwd"

    # resolve but providing values via env
    with custom_environ(
            {"INSTRUMENTED__HEAD": "h", "INSTRUMENTED__TUBE": '["tu", "u", "be"]', "INSTRUMENTED__HEELS": "xhe", "SECTIONED__PASSWORD": "passwd", "DEFAULT": "DEF"}):
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
                resolve.resolve_configuration(EmbeddedConfiguration(), explicit_value={"default": "set", "sectioned": {"password": "pwd"}})

    # part via env part via explicit values
    with custom_environ({"INSTRUMENTED__HEAD": "h", "INSTRUMENTED__TUBE": '["tu", "u", "be"]', "INSTRUMENTED__HEELS": "xhe"}):
        C = resolve.resolve_configuration(EmbeddedConfiguration(), explicit_value={"default": "set", "sectioned": {"password": "pwd"}})
        assert C.instrumented.to_native_representation() == "h>tu>u>be>xhe"


def test_embedded_explicit_value_over_provider(environment: Any) -> None:
    # make the instance sectioned so it can read from INSTRUMENTED
    with patch.object(InstrumentedConfiguration, "__section__", "instrumented"):
        with custom_environ({"INSTRUMENTED": "h>tu>u>be>he"}):
            # explicit value over the env
            c = resolve.resolve_configuration(EmbeddedConfiguration(), explicit_value={"instrumented": "h>tu>be>xhe"}, accept_partial=True)
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
            emb = InstrumentedConfiguration().parse_native_representation("h>tu>be>xhe")
            c = resolve.resolve_configuration(EmbeddedConfiguration(instrumented=emb), accept_partial=True)
            assert c.instrumented.to_native_representation() == "h>tu>u>be>he"
            # parent configuration is not resolved
            assert not c.is_resolved()
            assert c.is_partial()
            # but embedded is
            assert c.instrumented.__is_resolved__
            assert c.instrumented.is_resolved()
            assert not c.instrumented.is_partial()


def test_run_configuration_gen_name(environment: Any) -> None:
    C = resolve.resolve_configuration(RunConfiguration())
    assert C.pipeline_name.startswith("dlt_")


def test_configuration_is_mutable_mapping(environment: Any, env_provider: ConfigProvider) -> None:

    @configspec
    class _SecretCredentials(RunConfiguration):
        pipeline_name: Optional[str] = "secret"
        secret_value: TSecretValue = None
        config_files_storage_path: str = "storage"


    # configurations provide full MutableMapping support
    # here order of items in dict matters
    expected_dict = {
        'pipeline_name': 'secret',
        'sentry_dsn': None,
        'slack_incoming_hook': None,
        'dlthub_telemetry': True,
        'dlthub_telemetry_segment_write_key': 'TLJiyRkGVZGCi2TtjClamXpFcxAA1rSB',
        'log_format': '{asctime}|[{levelname:<21}]|{process}|{name}|{filename}|{funcName}:{lineno}|{message}',
        'log_level': 'WARNING',
        'request_timeout': 60,
        'request_max_attempts': 5,
        'request_backoff_factor': 1,
        'request_max_retry_delay': 300,
        'config_files_storage_path': 'storage',
        "secret_value": None
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
    c = SecretConfiguration()
    with pytest.raises(KeyError):
        c["unknown_prop"] = "unk"


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
    class MultiConfiguration(SectionedConfiguration, MockProdConfiguration, ConfigurationWithOptionalTypes):
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


def test_raises_on_many_unresolved_fields(environment: Any, env_provider: ConfigProvider) -> None:
    # via make configuration
    with pytest.raises(ConfigFieldMissingException) as cf_missing_exc:
        resolve.resolve_configuration(CoercionTestConfiguration())
    assert cf_missing_exc.value.spec_name == "CoercionTestConfiguration"
    # get all fields that must be set
    val_fields = [f for f in CoercionTestConfiguration().get_resolvable_fields() if f.lower().endswith("_val")]
    traces = cf_missing_exc.value.traces
    assert len(traces) == len(val_fields)
    for tr_field, exp_field in zip(traces, val_fields):
        assert len(traces[tr_field]) == 1
        assert traces[tr_field][0] == LookupTrace("Environment Variables", [], environ_provider.EnvironProvider.get_key_name(exp_field), None)
        # assert traces[tr_field][1] == LookupTrace("secrets.toml", [], toml.TomlFileProvider.get_key_name(exp_field), None)
        # assert traces[tr_field][2] == LookupTrace("config.toml", [], toml.TomlFileProvider.get_key_name(exp_field), None)


def test_accepts_optional_missing_fields(environment: Any) -> None:
    # ConfigurationWithOptionalTypes has values for all non optional fields present
    C = ConfigurationWithOptionalTypes()
    assert not C.is_partial()
    # make optional config
    resolve.resolve_configuration(ConfigurationWithOptionalTypes())
    # make config with optional values
    resolve.resolve_configuration(ProdConfigurationWithOptionalTypes(), explicit_value={"int_val": None})
    # make config with optional embedded config
    C = resolve.resolve_configuration(EmbeddedOptionalConfiguration())
    # embedded config was not fully resolved
    assert C.instrumented is None


def test_find_all_keys() -> None:
    keys = VeryWrongConfiguration().get_resolvable_fields()
    # assert hints and types: LOG_COLOR had it hint overwritten in derived class
    assert set({'str_val': str, 'int_val': int, 'NoneConfigVar': str, 'log_color': str}.items()).issubset(keys.items())


def test_coercion_to_hint_types(environment: Any) -> None:
    add_config_dict_to_env(COERCIONS)

    C = CoercionTestConfiguration()
    resolve._resolve_config_fields(C, explicit_values=None, explicit_sections=(), embedded_sections=(), accept_partial=False)

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
    credentials_str = "databricks+connector://token:<databricks_token>@<databricks_host>:443/<database_or_schema_name>?conn_timeout=15&search_path=a%2Cb%2Cc"
    credentials = deserialize_value("credentials", credentials_str, ConnectionStringCredentials)
    assert credentials.drivername == "databricks+connector"
    assert credentials.query == {"conn_timeout": "15", "search_path": "a,b,c"}
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
    add_config_dict_to_env(INVALID_COERCIONS)
    for key, value in INVALID_COERCIONS.items():
        try:
            resolve._resolve_config_fields(C, explicit_values=None, explicit_sections=(), embedded_sections=(), accept_partial=False)
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
    add_config_dict_to_env(COERCIONS)
    add_config_dict_to_env(EXCEPTED_COERCIONS, overwrite_keys=True)
    resolve._resolve_config_fields(C, explicit_values=None, explicit_sections=(), embedded_sections=(), accept_partial=False)
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


def test_config_with_non_templated_complex_hints(environment: Any) -> None:
    environment["LIST_VAL"] = "[1,2,3]"
    environment["TUPLE_VAL"] = "(1,2,3)"
    environment["DICT_VAL"] = '{"a": 1}'
    c = resolve.resolve_configuration(NonTemplatedComplexTypesConfiguration())
    assert c.list_val == [1,2,3]
    assert c.tuple_val == (1,2,3)
    assert c.dict_val == {"a": 1}


def test_resolve_configuration(environment: Any) -> None:
    # fill up configuration
    environment["NONECONFIGVAR"] = "1"
    C = resolve.resolve_configuration(WrongConfiguration())
    assert C.__is_resolved__
    assert C.NoneConfigVar == "1"


def test_dataclass_instantiation(environment: Any) -> None:
    # resolve_configuration works on instances of dataclasses and types are not modified
    environment['SECRET_VALUE'] = "1"
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
    assert is_valid_hint(Any) is True
    assert is_valid_hint(Optional[Any]) is True
    assert is_valid_hint(RunConfiguration) is True
    assert is_valid_hint(Optional[RunConfiguration]) is True
    assert is_valid_hint(TSecretValue) is True
    assert is_valid_hint(Optional[TSecretValue]) is True
    # in case of generics, origin will be used and args are not checked
    assert is_valid_hint(MutableMapping[TSecretValue, Any]) is True
    # this is valid (args not checked)
    assert is_valid_hint(MutableMapping[TSecretValue, ConfigValueCannotBeCoercedException]) is True
    assert is_valid_hint(Wei) is True
    # any class type, except deriving from BaseConfiguration is wrong type
    assert is_valid_hint(ConfigFieldMissingException) is False


def test_configspec_auto_base_config_derivation() -> None:

    @configspec(init=True)
    class AutoBaseDerivationConfiguration:
        auto: str

    assert issubclass(AutoBaseDerivationConfiguration, BaseConfiguration)
    assert hasattr(AutoBaseDerivationConfiguration, "auto")

    assert AutoBaseDerivationConfiguration().auto is None
    assert AutoBaseDerivationConfiguration(auto="auto").auto == "auto"
    assert AutoBaseDerivationConfiguration(auto="auto").get_resolvable_fields() == {"auto": str}
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
    c = SecretConfiguration()
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(c)
    assert c.__exception__ is py_ex.value
    # but if ran again exception is cleared
    environment["SECRET_VALUE"] = "password"
    resolve.resolve_configuration(c)
    assert c.__exception__ is None
    # explicit value
    c = InstrumentedConfiguration()
    with pytest.raises(InvalidNativeValue) as py_ex:
        resolve.resolve_configuration(c, explicit_value=2137)
    assert c.__exception__ is py_ex.value


def test_resolved_trace(environment: Any) -> None:
    with custom_environ(
            {"INSTRUMENTED__HEAD": "h", "INSTRUMENTED__TUBE": '["tu", "u", "be"]', "INSTRUMENTED__HEELS": "xhe", "SECTIONED__PASSWORD": "passwd", "DEFAULT": "DEF"}):
        c = resolve.resolve_configuration(EmbeddedConfiguration(default="_DEFF"))
    traces = get_resolved_traces()
    prov_name = environ_provider.EnvironProvider().name
    assert traces[".default"] == ResolvedValueTrace("default", "DEF", "_DEFF", str, [], prov_name, c)
    assert traces["instrumented.head"] == ResolvedValueTrace("head", "h", None, str, ["instrumented"], prov_name, c.instrumented)
    # value is before casting
    assert traces["instrumented.tube"] == ResolvedValueTrace("tube", '["tu", "u", "be"]', None, List[str], ["instrumented"], prov_name, c.instrumented)
    assert deserialize_value("tube", traces["instrumented.tube"].value, resolve.extract_inner_hint(List[str])) == ["tu", "u", "be"]
    assert traces["instrumented.heels"] == ResolvedValueTrace("heels", "xhe", None, str, ["instrumented"], prov_name, c.instrumented)
    assert traces["sectioned.password"] == ResolvedValueTrace("password", "passwd", None, str, ["sectioned"], prov_name, c.sectioned)
    assert len(traces) == 5

    # try to get native representation
    with patch.object(InstrumentedConfiguration, "__section__", "snake"):
        with custom_environ(
                {"INSTRUMENTED": "h>t>t>t>he", "SECTIONED__PASSWORD": "pass", "DEFAULT": "UNDEF", "SNAKE": "h>t>t>t>he"}):
            c = resolve.resolve_configuration(EmbeddedConfiguration())
            resolve.resolve_configuration(InstrumentedConfiguration())

    assert traces[".default"] == ResolvedValueTrace("default", "UNDEF", None, str, [], prov_name, c)
    assert traces[".instrumented"] == ResolvedValueTrace("instrumented", "h>t>t>t>he", None, InstrumentedConfiguration, [], prov_name, c)

    assert traces[".snake"] == ResolvedValueTrace("snake", "h>t>t>t>he", None, InstrumentedConfiguration, [], prov_name, None)


def test_extract_inner_hint() -> None:
    # extracts base config from an union
    assert resolve.extract_inner_hint(Union[GcpServiceAccountCredentialsWithoutDefaults, StrAny, str]) is GcpServiceAccountCredentialsWithoutDefaults
    assert resolve.extract_inner_hint(Union[InstrumentedConfiguration, StrAny, str]) is InstrumentedConfiguration
    # keeps unions
    assert resolve.extract_inner_hint(Union[StrAny, str]) is Union
    # ignores specialization in list and dict, leaving origin
    assert resolve.extract_inner_hint(List[str]) is list
    assert resolve.extract_inner_hint(DictStrAny) is dict
    # extracts new types
    assert resolve.extract_inner_hint(TSecretValue) is AnyType
    # preserves new types on extract
    assert resolve.extract_inner_hint(TSecretValue, preserve_new_types=True) is TSecretValue


def test_is_secret_hint() -> None:
    assert resolve.is_secret_hint(GcpServiceAccountCredentialsWithoutDefaults) is True
    assert resolve.is_secret_hint(Optional[GcpServiceAccountCredentialsWithoutDefaults]) is True
    assert resolve.is_secret_hint(TSecretValue) is True
    assert resolve.is_secret_hint(Optional[TSecretValue]) is True
    assert resolve.is_secret_hint(InstrumentedConfiguration) is False
    # do not recognize new types
    TTestSecretNt = NewType("TTestSecretNt", GcpServiceAccountCredentialsWithoutDefaults)
    assert resolve.is_secret_hint(TTestSecretNt) is False
    # recognize unions with credentials
    assert resolve.is_secret_hint(Union[GcpServiceAccountCredentialsWithoutDefaults, StrAny, str]) is True
    # we do not recognize unions if they do not contain configuration types
    assert resolve.is_secret_hint(Union[TSecretValue, StrAny, str]) is False
    assert resolve.is_secret_hint(Optional[str]) is False
    assert resolve.is_secret_hint(str) is False
    assert resolve.is_secret_hint(AnyType) is False


def test_is_secret_hint_custom_type() -> None:
    # any new type named TSecretValue is a secret
    assert resolve.is_secret_hint(NewType("TSecretValue", int)) is True
    assert resolve.is_secret_hint(NewType("TSecretValueX", int)) is False


def coerce_single_value(key: str, value: str, hint: Type[Any]) -> Any:
    hint = extract_inner_type(hint)
    return resolve.deserialize_value(key, value, hint)


def test_dynamic_type_hint(environment: Dict[str, str]) -> None:
    """Test dynamic type hint using @resolve_type decorator
    """
    environment['DUMMY__DISCRIMINATOR'] = 'b'
    environment['DUMMY__EMBEDDED_CONFIG__FIELD_FOR_B'] = 'some_value'

    config = resolve.resolve_configuration(ConfigWithDynamicType(), sections=('dummy', ))

    assert isinstance(config.embedded_config, DynamicConfigB)
    assert config.embedded_config.field_for_b == 'some_value'


def test_dynamic_type_hint_subclass(environment: Dict[str, str]) -> None:
    """Test overriding @resolve_type method in subclass
    """
    environment['DUMMY__IS_NUMBER'] = 'true'
    environment['DUMMY__DYNAMIC_TYPE_FIELD'] = '22'

    # Test extended resolver method is applied
    environment['DUMMY__DISCRIMINATOR'] = 'c'
    environment['DUMMY__EMBEDDED_CONFIG__FIELD_FOR_C'] = 'some_value'

    config = resolve.resolve_configuration(SubclassConfigWithDynamicType(), sections=('dummy', ))

    assert isinstance(config.embedded_config, DynamicConfigC)
    assert config.embedded_config.field_for_c == 'some_value'

    # Test super() call is applied correctly
    environment['DUMMY__DISCRIMINATOR'] = 'b'
    environment['DUMMY__EMBEDDED_CONFIG__FIELD_FOR_B'] = 'some_value'

    config = resolve.resolve_configuration(SubclassConfigWithDynamicType(), sections=('dummy', ))

    assert isinstance(config.embedded_config, DynamicConfigB)
    assert config.embedded_config.field_for_b == 'some_value'

    # Test second dynamic field added in subclass
    environment['DUMMY__IS_NUMBER'] = 'true'
    environment['DUMMY__DYNAMIC_TYPE_FIELD'] = 'some'

    with pytest.raises(ConfigValueCannotBeCoercedException) as e:
        config = resolve.resolve_configuration(SubclassConfigWithDynamicType(), sections=('dummy', ))

    assert e.value.field_name == 'dynamic_type_field'
    assert e.value.hint == int


def test_unmatched_dynamic_hint_resolvers(environment: Dict[str, str]) -> None:
    with pytest.raises(UnmatchedConfigHintResolversException) as e:
        resolve.resolve_configuration(ConfigWithInvalidDynamicType())

    print(e.value)

    assert set(e.value.field_names) == {"a", "b", "c"}
    assert e.value.spec_name == ConfigWithInvalidDynamicType.__name__
