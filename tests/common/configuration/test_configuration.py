import pytest
import datetime  # noqa: I251
from typing import Any, Dict, List, Mapping, MutableMapping, NewType, Optional, Type

from dlt.common import pendulum, Decimal, Wei
from dlt.common.utils import custom_environ
from dlt.common.typing import TSecretValue, extract_inner_type
from dlt.common.configuration.exceptions import ConfigFieldMissingTypeHintException, ConfigFieldTypeHintNotSupported, InvalidInitialValue, LookupTrace, ValueNotSecretException
from dlt.common.configuration import configspec, ConfigFieldMissingException, ConfigValueCannotBeCoercedException, resolve
from dlt.common.configuration.specs import BaseConfiguration, RunConfiguration
from dlt.common.configuration.specs.base_configuration import is_valid_hint
from dlt.common.configuration.providers import environ as environ_provider, toml

from tests.utils import preserve_environ, add_config_dict_to_env
from tests.common.configuration.utils import MockProvider, CoercionTestConfiguration, COERCIONS, WithCredentialsConfiguration, WrongConfiguration, SecretConfiguration, NamespacedConfiguration, environment, mock_provider

INVALID_COERCIONS = {
    # 'STR_VAL': 'test string',  # string always OK
    'int_val': "a12345",
    'bool_val': "not_bool",  # bool overridden by string - that is the most common problem
    'list_val': {2: 1, "2": 3.0},
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

    def from_native_representation(self, native_value: Any) -> None:
        if not isinstance(native_value, str):
            raise ValueError(native_value)
        parts = native_value.split(">")
        self.head = parts[0]
        self.heels = parts[-1]
        self.tube = parts[1:-1]

    def check_integrity(self) -> None:
        if self.head > self.heels:
            raise RuntimeError("Head over heels")


@configspec
class EmbeddedConfiguration(BaseConfiguration):
    default: str
    instrumented: InstrumentedConfiguration
    namespaced: NamespacedConfiguration


@configspec
class EmbeddedOptionalConfiguration(BaseConfiguration):
    instrumented: Optional[InstrumentedConfiguration]


@configspec
class EmbeddedSecretConfiguration(BaseConfiguration):
    secret: SecretConfiguration


LongInteger = NewType("LongInteger", int)
FirstOrderStr = NewType("FirstOrderStr", str)
SecondOrderStr = NewType("SecondOrderStr", FirstOrderStr)


def test_initial_config_state() -> None:
    assert BaseConfiguration.__is_resolved__ is False
    assert BaseConfiguration.__namespace__ is None
    c = BaseConfiguration()
    assert c.__is_resolved__ is False
    assert c.is_resolved() is False
    # base configuration has no resolvable fields so is never partial
    assert c.is_partial() is False


def test_set_initial_config_value(environment: Any) -> None:
    # set from init method
    c = resolve.resolve_configuration(InstrumentedConfiguration(head="h", tube=["a", "b"], heels="he"))
    assert c.to_native_representation() == "h>a>b>he"
    # set from native form
    c = resolve.resolve_configuration(InstrumentedConfiguration(), initial_value="h>a>b>he")
    assert c.head == "h"
    assert c.tube == ["a", "b"]
    assert c.heels == "he"
    # set from dictionary
    c = resolve.resolve_configuration(InstrumentedConfiguration(), initial_value={"head": "h", "tube": ["tu", "be"], "heels": "xhe"})
    assert c.to_native_representation() == "h>tu>be>xhe"


def test_initial_native_representation_skips_resolve(environment: Any) -> None:
    c = InstrumentedConfiguration()
    # mock namespace to enable looking for initials in provider
    c.__namespace__ = "ins"
    # explicit initial does not skip resolve
    environment["INS__HEELS"] = "xhe"
    c = resolve.resolve_configuration(c, initial_value="h>a>b>he")
    assert c.heels == "xhe"

    # now put the whole native representation in env
    environment["INS"] = "h>a>b>he"
    c = InstrumentedConfiguration()
    c.__namespace__ = "ins"
    c = resolve.resolve_configuration(c, initial_value="h>a>b>uhe")
    assert c.heels == "he"


def test_query_initial_config_value_if_config_namespace(environment: Any) -> None:
    c = InstrumentedConfiguration(head="h", tube=["a", "b"], heels="he")
    # mock the __namespace__ to enable the query
    c.__namespace__ = "snake"
    # provide the initial value
    environment["SNAKE"] = "h>tu>be>xhe"
    c = resolve.resolve_configuration(c)
    # check if the initial value loaded
    assert c.heels == "xhe"


def test_invalid_initial_config_value() -> None:
    # 2137 cannot be parsed and also is not a dict that can initialize the fields
    with pytest.raises(InvalidInitialValue) as py_ex:
        resolve.resolve_configuration(InstrumentedConfiguration(), initial_value=2137)
    assert py_ex.value.spec is InstrumentedConfiguration
    assert py_ex.value.initial_value_type is int


def test_check_integrity(environment: Any) -> None:
    with pytest.raises(RuntimeError):
        # head over hells
        resolve.resolve_configuration(InstrumentedConfiguration(), initial_value="he>a>b>h")


def test_embedded_config(environment: Any) -> None:
    # resolve all embedded config, using initial value for instrumented config and initial dict for namespaced config
    C = resolve.resolve_configuration(EmbeddedConfiguration(), initial_value={"default": "set", "instrumented": "h>tu>be>xhe", "namespaced": {"password": "pwd"}})
    assert C.default == "set"
    assert C.instrumented.to_native_representation() == "h>tu>be>xhe"
    assert C.namespaced.password == "pwd"

    # resolve but providing values via env
    with custom_environ({"INSTRUMENTED": "h>tu>u>be>xhe", "NAMESPACED__PASSWORD": "passwd", "DEFAULT": "DEF"}):
        C = resolve.resolve_configuration(EmbeddedConfiguration())
        assert C.default == "DEF"
        assert C.instrumented.to_native_representation() == "h>tu>u>be>xhe"
        assert C.namespaced.password == "passwd"

    # resolve partial, partial is passed to embedded
    C = resolve.resolve_configuration(EmbeddedConfiguration(), accept_partial=True)
    assert not C.__is_resolved__
    assert not C.namespaced.__is_resolved__
    assert not C.instrumented.__is_resolved__

    # some are partial, some are not
    with custom_environ({"NAMESPACED__PASSWORD": "passwd"}):
        C = resolve.resolve_configuration(EmbeddedConfiguration(), accept_partial=True)
        assert not C.__is_resolved__
        assert C.namespaced.__is_resolved__
        assert not C.instrumented.__is_resolved__

    # single integrity error fails all the embeds
    with custom_environ({"INSTRUMENTED": "he>tu>u>be>h"}):
        with pytest.raises(RuntimeError):
            resolve.resolve_configuration(EmbeddedConfiguration(), initial_value={"default": "set", "namespaced": {"password": "pwd"}})

    # part via env part via initial values
    with custom_environ({"INSTRUMENTED": "h>tu>u>be>he"}):
        C = resolve.resolve_configuration(EmbeddedConfiguration(), initial_value={"default": "set", "namespaced": {"password": "pwd"}})
        assert C.instrumented.to_native_representation() == "h>tu>u>be>he"


def test_provider_values_over_initial(environment: Any) -> None:
    with custom_environ({"INSTRUMENTED": "h>tu>u>be>he"}):
        C = resolve.resolve_configuration(EmbeddedConfiguration(), initial_value={"instrumented": "h>tu>be>xhe"}, accept_partial=True)
        assert C.instrumented.to_native_representation() == "h>tu>u>be>he"
        # parent configuration is not resolved
        assert not C.is_resolved()
        assert C.is_partial()
        # but embedded is
        assert C.instrumented.__is_resolved__
        assert C.instrumented.is_resolved()
        assert not C.instrumented.is_partial()


def test_run_configuration_gen_name(environment: Any) -> None:
    C = resolve.resolve_configuration(RunConfiguration())
    assert C.pipeline_name.startswith("dlt_")


def test_configuration_is_mutable_mapping(environment: Any) -> None:


    @configspec
    class _SecretCredentials(RunConfiguration):
        pipeline_name: Optional[str] = "secret"
        secret_value: TSecretValue = None


    # configurations provide full MutableMapping support
    # here order of items in dict matters
    expected_dict = {
        'pipeline_name': 'secret',
        'sentry_dsn': None,
        'prometheus_port': None,
        'log_format': '{asctime}|[{levelname:<21}]|{process}|{name}|{filename}|{funcName}:{lineno}|{message}',
        'log_level': 'DEBUG',
        'request_timeout': (15, 300),
        'config_files_storage_path': '_storage/config/%s',
        "secret_value": None
    }
    assert dict(_SecretCredentials()) == expected_dict

    environment["SECRET_VALUE"] = "secret"
    C = resolve.resolve_configuration(_SecretCredentials())
    expected_dict["secret_value"] = "secret"
    assert dict(C) == expected_dict

    # check mutable mapping type
    assert isinstance(C, MutableMapping)
    assert isinstance(C, Mapping)
    assert not isinstance(C, Dict)

    # check view ops
    assert C.keys() == expected_dict.keys()
    assert len(C) == len(expected_dict)
    assert C.items() == expected_dict.items()
    assert list(C.values()) == list(expected_dict.values())
    for key in C:
        assert C[key] == expected_dict[key]
    # version is present as attr but not present in dict
    assert hasattr(C, "__is_resolved__")
    assert hasattr(C, "__namespace__")

    # set ops
    # update supported and non existing attributes are ignored
    C.update({"pipeline_name": "old pipe", "__version": "1.1.1"})
    assert C.pipeline_name == "old pipe" == C["pipeline_name"]

    # delete is not supported
    with pytest.raises(KeyError):
        del C["pipeline_name"]

    with pytest.raises(KeyError):
        C.pop("pipeline_name", None)

    # setting supported
    C["pipeline_name"] = "new pipe"
    assert C.pipeline_name == "new pipe" == C["pipeline_name"]


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
    class MultiConfiguration(MockProdConfiguration, ConfigurationWithOptionalTypes, NamespacedConfiguration):
        pass

    # apparently dataclasses set default in reverse mro so MockProdConfiguration overwrites
    C = MultiConfiguration()
    assert C.pipeline_name == MultiConfiguration.pipeline_name == "comp"
    # but keys are ordered in MRO so password from NamespacedConfiguration goes first
    keys = list(C.keys())
    assert keys[0] == "password"
    assert keys[-1] == "bool_val"
    assert C.__namespace__ == "DLT_TEST"


def test_raises_on_unresolved_field(environment: Any) -> None:
    # via make configuration
    with pytest.raises(ConfigFieldMissingException) as cf_missing_exc:
        resolve.resolve_configuration(WrongConfiguration())
    assert cf_missing_exc.value.spec_name == "WrongConfiguration"
    assert "NoneConfigVar" in cf_missing_exc.value.traces
    # has only one trace
    trace = cf_missing_exc.value.traces["NoneConfigVar"]
    assert len(trace) == 3
    assert trace[0] == LookupTrace("Environment Variables", [], "NONECONFIGVAR", None)
    assert trace[1] == LookupTrace("Pipeline secrets.toml", [], "NoneConfigVar", None)
    assert trace[2] == LookupTrace("Pipeline config.toml", [], "NoneConfigVar", None)


def test_raises_on_many_unresolved_fields(environment: Any) -> None:
    # via make configuration
    with pytest.raises(ConfigFieldMissingException) as cf_missing_exc:
        resolve.resolve_configuration(CoercionTestConfiguration())
    assert cf_missing_exc.value.spec_name == "CoercionTestConfiguration"
    # get all fields that must be set
    val_fields = [f for f in CoercionTestConfiguration().get_resolvable_fields() if f.lower().endswith("_val")]
    traces = cf_missing_exc.value.traces
    assert len(traces) == len(val_fields)
    for tr_field, exp_field in zip(traces, val_fields):
        assert len(traces[tr_field]) == 3
        assert traces[tr_field][0] == LookupTrace("Environment Variables", [], environ_provider.EnvironProvider.get_key_name(exp_field), None)
        assert traces[tr_field][1] == LookupTrace("Pipeline secrets.toml", [], toml.TomlProvider.get_key_name(exp_field), None)
        assert traces[tr_field][2] == LookupTrace("Pipeline config.toml", [], toml.TomlProvider.get_key_name(exp_field), None)


def test_accepts_optional_missing_fields(environment: Any) -> None:
    # ConfigurationWithOptionalTypes has values for all non optional fields present
    C = ConfigurationWithOptionalTypes()
    assert not C.is_partial()
    # make optional config
    resolve.resolve_configuration(ConfigurationWithOptionalTypes())
    # make config with optional values
    resolve.resolve_configuration(ProdConfigurationWithOptionalTypes(), initial_value={"int_val": None})
    # make config with optional embedded config
    C = resolve.resolve_configuration(EmbeddedOptionalConfiguration())
    # embedded config was not fully resolved
    assert not C.instrumented.__is_resolved__
    assert not C.instrumented.is_resolved()
    assert C.instrumented.is_partial()


def test_find_all_keys() -> None:
    keys = VeryWrongConfiguration().get_resolvable_fields()
    # assert hints and types: LOG_COLOR had it hint overwritten in derived class
    assert set({'str_val': str, 'int_val': int, 'NoneConfigVar': str, 'log_color': str}.items()).issubset(keys.items())


def test_coercion_to_hint_types(environment: Any) -> None:
    add_config_dict_to_env(COERCIONS)

    C = CoercionTestConfiguration()
    resolve._resolve_config_fields(C, explicit_namespaces=(), embedded_namespaces=(), accept_partial=False)

    for key in COERCIONS:
        assert getattr(C, key) == COERCIONS[key]


def test_values_serialization() -> None:
    # test tuple
    t_tuple = (1, 2, 3, "A")
    v = resolve.serialize_value(t_tuple)
    assert v == "(1, 2, 3, 'A')"  # literal serialization
    assert resolve.deserialize_value("K", v, tuple) == t_tuple

    # test list
    t_list = ["a", 3, True]
    v = resolve.serialize_value(t_list)
    assert v == '["a", 3, true]'  # json serialization
    assert resolve.deserialize_value("K", v, list) == t_list

    # test datetime
    t_date = pendulum.now()
    v = resolve.serialize_value(t_date)
    assert resolve.deserialize_value("K", v, datetime.datetime) == t_date

    # test wei
    t_wei = Wei.from_int256(10**16, decimals=18)
    v = resolve.serialize_value(t_wei)
    assert v == "0.01"
    # can be deserialized into
    assert resolve.deserialize_value("K", v, float) == 0.01
    assert resolve.deserialize_value("K", v, Decimal) == Decimal("0.01")
    assert resolve.deserialize_value("K", v, Wei) == Wei("0.01")


def test_invalid_coercions(environment: Any) -> None:
    C = CoercionTestConfiguration()
    add_config_dict_to_env(INVALID_COERCIONS)
    for key, value in INVALID_COERCIONS.items():
        try:
            resolve._resolve_config_fields(C, explicit_namespaces=(), embedded_namespaces=(), accept_partial=False)
        except ConfigValueCannotBeCoercedException as coerc_exc:
            # must fail exactly on expected value
            if coerc_exc.field_name != key:
                raise
            # overwrite with valid value and go to next env
            environment[key.upper()] = resolve.serialize_value(COERCIONS[key])
            continue
        raise AssertionError("%s was coerced with %s which is invalid type" % (key, value))


def test_excepted_coercions(environment: Any) -> None:
    C = CoercionTestConfiguration()
    add_config_dict_to_env(COERCIONS)
    add_config_dict_to_env(EXCEPTED_COERCIONS, overwrite_keys=True)
    resolve._resolve_config_fields(C, explicit_namespaces=(), embedded_namespaces=(), accept_partial=False)
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


def test_initial_values(environment: Any) -> None:
    # initial values will be overridden from env
    environment["PIPELINE_NAME"] = "env name"
    environment["CREATED_VAL"] = "12837"
    # set initial values and allow partial config
    C = resolve.resolve_configuration(CoercionTestConfiguration(),
        initial_value={"pipeline_name": "initial name", "none_val": type(environment), "created_val": 878232, "bytes_val": b"str"},
        accept_partial=True
    )
    # from env
    assert C.pipeline_name == "env name"
    # from initial
    assert C.bytes_val == b"str"
    assert C.none_val == type(environment)
    # new prop overridden from env
    assert environment["CREATED_VAL"] == "12837"


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
        resolve.resolve_configuration(SecretConfiguration(), namespaces=("mock",))
    assert py_ex.value.provider_name == "Mock Provider"
    assert py_ex.value.key == "-secret_value"

    # anything derived from CredentialsConfiguration will fail
    with pytest.raises(ValueNotSecretException) as py_ex:
        resolve.resolve_configuration(WithCredentialsConfiguration(), namespaces=("mock",))
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
    resolve.resolve_configuration(c).secret_value == "else"


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
    # initial value
    c = InstrumentedConfiguration()
    with pytest.raises(InvalidInitialValue) as py_ex:
        resolve.resolve_configuration(c, initial_value=2137)
    assert c.__exception__ is py_ex.value


def coerce_single_value(key: str, value: str, hint: Type[Any]) -> Any:
    hint = extract_inner_type(hint)
    return resolve.deserialize_value(key, value, hint)
