import pytest
from os import environ
import datetime  # noqa: I251
from typing import Any, Dict, List, Mapping, MutableMapping, NewType, Optional, Sequence, Tuple, Type

from dlt.common import pendulum, Decimal, Wei
from dlt.common.configuration.exceptions import ConfigFieldMissingTypeHintException, ConfigFieldTypeHintNotSupported, LookupTrace
from dlt.common.typing import StrAny, TSecretValue, extract_inner_type
from dlt.common.configuration import configspec, ConfigEntryMissingException, ConfigFileNotFoundException, ConfigEnvValueCannotBeCoercedException, resolve
from dlt.common.configuration.specs import BaseConfiguration, RunConfiguration
from dlt.common.configuration.providers import environ as environ_provider
from dlt.common.utils import custom_environ

from tests.utils import preserve_environ, add_config_dict_to_env

# used to test version
__version__ = "1.0.5"

COERCIONS = {
    'str_val': 'test string',
    'int_val': 12345,
    'bool_val': True,
    'list_val': [1, "2", [3]],
    'dict_val': {
        'a': 1,
        "b": "2"
    },
    'bytes_val': b'Hello World!',
    'float_val': 1.18927,
    "tuple_val": (1, 2, {1: "complicated dicts allowed in literal eval"}),
    'any_val': "function() {}",
    'none_val': "none",
    'COMPLEX_VAL': {
        "_": [1440, ["*"], []],
        "change-email": [560, ["*"], []]
    },
    "date_val": pendulum.now(),
    "dec_val": Decimal("22.38"),
    "sequence_val": ["A", "B", "KAPPA"],
    "gen_list_val": ["C", "Z", "N"],
    "mapping_val": {"FL": 1, "FR": {"1": 2}},
    "mutable_mapping_val": {"str": "str"}
}

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
class SimpleConfiguration(RunConfiguration):
    pipeline_name: str = "Some Name"
    test_bool: bool = False


@configspec
class WrongConfiguration(RunConfiguration):
    pipeline_name: str = "Some Name"
    NoneConfigVar: str = None
    log_color: bool = True


@configspec
class SecretConfiguration(RunConfiguration):
    pipeline_name: str = "secret"
    secret_value: TSecretValue = None


@configspec
class SecretKubeConfiguration(RunConfiguration):
    pipeline_name: str = "secret kube"
    secret_kube: TSecretValue = None


@configspec
class CoercionTestConfiguration(RunConfiguration):
    pipeline_name: str = "Some Name"
    str_val: str = None
    int_val: int = None
    bool_val: bool = None
    list_val: list = None  # type: ignore
    dict_val: dict = None  # type: ignore
    bytes_val: bytes = None
    float_val: float = None
    tuple_val: Tuple[int, int, StrAny] = None
    any_val: Any = None
    none_val: str = None
    COMPLEX_VAL: Dict[str, Tuple[int, List[str], List[str]]] = None
    date_val: datetime.datetime = None
    dec_val: Decimal = None
    sequence_val: Sequence[str] = None
    gen_list_val: List[str] = None
    mapping_val: StrAny = None
    mutable_mapping_val: MutableMapping[str, str] = None



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


@configspec
class MockProdConfigurationVar(RunConfiguration):
    pipeline_name: str = "comp"


@configspec
class NamespacedConfiguration(BaseConfiguration):
    __namespace__ = "DLT_TEST"

    password: str = None


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


LongInteger = NewType("LongInteger", int)
FirstOrderStr = NewType("FirstOrderStr", str)
SecondOrderStr = NewType("SecondOrderStr", FirstOrderStr)


@pytest.fixture(scope="function")
def environment() -> Any:
    environ.clear()
    return environ


def test_initial_config_state() -> None:
    assert BaseConfiguration.__is_resolved__ is False
    assert BaseConfiguration.__namespace__ is None
    C = BaseConfiguration()
    assert C.__is_resolved__ is False
    assert C.is_resolved() is False
    # base configuration has no resolvable fields so is never partial
    assert C.is_partial() is False


def test_set_initial_config_value(environment: Any) -> None:
    # set from init method
    C = resolve.make_configuration(InstrumentedConfiguration(head="h", tube=["a", "b"], heels="he"))
    assert C.to_native_representation() == "h>a>b>he"
    # set from native form
    C = resolve.make_configuration(InstrumentedConfiguration(), initial_value="h>a>b>he")
    assert C.head == "h"
    assert C.tube == ["a", "b"]
    assert C.heels == "he"
    # set from dictionary
    C = resolve.make_configuration(InstrumentedConfiguration(), initial_value={"head": "h", "tube": ["tu", "be"], "heels": "xhe"})
    assert C.to_native_representation() == "h>tu>be>xhe"


def test_check_integrity(environment: Any) -> None:
    with pytest.raises(RuntimeError):
        # head over hells
        resolve.make_configuration(InstrumentedConfiguration(), initial_value="he>a>b>h")


def test_embedded_config(environment: Any) -> None:
    # resolve all embedded config, using initial value for instrumented config and initial dict for namespaced config
    C = resolve.make_configuration(EmbeddedConfiguration(), initial_value={"default": "set", "instrumented": "h>tu>be>xhe", "namespaced": {"password": "pwd"}})
    assert C.default == "set"
    assert C.instrumented.to_native_representation() == "h>tu>be>xhe"
    assert C.namespaced.password == "pwd"

    # resolve but providing values via env
    with custom_environ({"INSTRUMENTED": "h>tu>u>be>xhe", "DLT_TEST__PASSWORD": "passwd", "DEFAULT": "DEF"}):
        C = resolve.make_configuration(EmbeddedConfiguration())
        assert C.default == "DEF"
        assert C.instrumented.to_native_representation() == "h>tu>u>be>xhe"
        assert C.namespaced.password == "passwd"

    # resolve partial, partial is passed to embedded
    C = resolve.make_configuration(EmbeddedConfiguration(), accept_partial=True)
    assert not C.__is_resolved__
    assert not C.namespaced.__is_resolved__
    assert not C.instrumented.__is_resolved__

    # some are partial, some are not
    with custom_environ({"DLT_TEST__PASSWORD": "passwd"}):
        C = resolve.make_configuration(EmbeddedConfiguration(), accept_partial=True)
        assert not C.__is_resolved__
        assert C.namespaced.__is_resolved__
        assert not C.instrumented.__is_resolved__

    # single integrity error fails all the embeds
    with custom_environ({"INSTRUMENTED": "he>tu>u>be>h"}):
        with pytest.raises(RuntimeError):
            resolve.make_configuration(EmbeddedConfiguration(), initial_value={"default": "set", "namespaced": {"password": "pwd"}})

    # part via env part via initial values
    with custom_environ({"INSTRUMENTED": "h>tu>u>be>he"}):
        C = resolve.make_configuration(EmbeddedConfiguration(), initial_value={"default": "set", "namespaced": {"password": "pwd"}})
        assert C.instrumented.to_native_representation() == "h>tu>u>be>he"


def test_provider_values_over_initial(environment: Any) -> None:
    with custom_environ({"INSTRUMENTED": "h>tu>u>be>he"}):
        C = resolve.make_configuration(EmbeddedConfiguration(), initial_value={"instrumented": "h>tu>be>xhe"}, accept_partial=True)
        assert C.instrumented.to_native_representation() == "h>tu>u>be>he"
        # parent configuration is not resolved
        assert not C.is_resolved()
        assert C.is_partial()
        # but embedded is
        assert C.instrumented.__is_resolved__
        assert C.instrumented.is_resolved()
        assert not C.instrumented.is_partial()


def test_run_configuration_gen_name(environment: Any) -> None:
    C = resolve.make_configuration(RunConfiguration())
    assert C.pipeline_name.startswith("dlt_")


def test_configuration_is_mutable_mapping(environment: Any) -> None:
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
        'secret_value': None
    }
    assert dict(SecretConfiguration()) == expected_dict

    environment["SECRET_VALUE"] = "secret"
    C = resolve.make_configuration(SecretConfiguration())
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
    assert hasattr(C, "_version")
    assert hasattr(C, "__is_resolved__")
    assert hasattr(C, "__namespace__")

    with pytest.raises(KeyError):
        C["_version"]

    # set ops
    # update supported and non existing attributes are ignored
    C.update({"pipeline_name": "old pipe", "__version": "1.1.1"})
    assert C.pipeline_name == "old pipe" == C["pipeline_name"]
    assert C._version != "1.1.1"

    # delete is not supported
    with pytest.raises(KeyError):
        del C["pipeline_name"]

    with pytest.raises(KeyError):
        C.pop("pipeline_name", None)

    # setting supported
    C["pipeline_name"] = "new pipe"
    assert C.pipeline_name == "new pipe" == C["pipeline_name"]

    with pytest.raises(KeyError):
        C["_version"] = "1.1.1"


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
    with pytest.raises(ConfigEntryMissingException) as cf_missing_exc:
        resolve.make_configuration(WrongConfiguration())
    assert cf_missing_exc.value.spec_name == "WrongConfiguration"
    assert "NoneConfigVar" in cf_missing_exc.value.traces
    # has only one trace
    trace = cf_missing_exc.value.traces["NoneConfigVar"]
    assert len(trace) == 1
    assert trace[0] == LookupTrace("Environment Variables", [], "NONECONFIGVAR", None)


def test_raises_on_many_unresolved_fields(environment: Any) -> None:
    # via make configuration
    with pytest.raises(ConfigEntryMissingException) as cf_missing_exc:
        resolve.make_configuration(CoercionTestConfiguration())
    assert cf_missing_exc.value.spec_name == "CoercionTestConfiguration"
    # get all fields that must be set
    val_fields = [f for f in CoercionTestConfiguration().get_resolvable_fields() if f.lower().endswith("_val")]
    traces = cf_missing_exc.value.traces
    assert len(traces) == len(val_fields)
    for tr_field, exp_field in zip(traces, val_fields):
        assert len(traces[tr_field]) == 1
        assert traces[tr_field][0] == LookupTrace("Environment Variables", [], environ_provider.EnvironProvider.get_key_name(exp_field), None)


def test_accepts_optional_missing_fields(environment: Any) -> None:
    # ConfigurationWithOptionalTypes has values for all non optional fields present
    C = ConfigurationWithOptionalTypes()
    assert not C.is_partial()
    # make optional config
    resolve.make_configuration(ConfigurationWithOptionalTypes())
    # make config with optional values
    resolve.make_configuration(ProdConfigurationWithOptionalTypes(), initial_value={"int_val": None})
    # make config with optional embedded config
    C = resolve.make_configuration(EmbeddedOptionalConfiguration())
    # embedded config was not fully resolved
    assert not C.instrumented.__is_resolved__
    assert not C.instrumented.is_resolved()
    assert C.instrumented.is_partial()


def test_resolves_from_environ(environment: Any) -> None:
    environment["NONECONFIGVAR"] = "Some"

    C = WrongConfiguration()
    resolve._resolve_config_fields(C, accept_partial=False)
    assert not C.is_partial()

    assert C.NoneConfigVar == environment["NONECONFIGVAR"]


def test_resolves_from_environ_with_coercion(environment: Any) -> None:
    environment["TEST_BOOL"] = 'yes'

    C = SimpleConfiguration()
    resolve._resolve_config_fields(C, accept_partial=False)
    assert not C.is_partial()

    # value will be coerced to bool
    assert C.test_bool is True


def test_find_all_keys() -> None:
    keys = VeryWrongConfiguration().get_resolvable_fields()
    # assert hints and types: LOG_COLOR had it hint overwritten in derived class
    assert set({'str_val': str, 'int_val': int, 'NoneConfigVar': str, 'log_color': str}.items()).issubset(keys.items())


def test_coercion_to_hint_types(environment: Any) -> None:
    add_config_dict_to_env(COERCIONS)

    C = CoercionTestConfiguration()
    resolve._resolve_config_fields(C, accept_partial=False)

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
            resolve._resolve_config_fields(C, accept_partial=False)
        except ConfigEnvValueCannotBeCoercedException as coerc_exc:
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
    resolve._resolve_config_fields(C, accept_partial=False)
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





def test_make_configuration(environment: Any) -> None:
    # fill up configuration
    environment["NONECONFIGVAR"] = "1"
    C = resolve.make_configuration(WrongConfiguration())
    assert C.__is_resolved__
    assert C.NoneConfigVar == "1"


def test_auto_derivation(environment: Any) -> None:
    # make_configuration works on instances of dataclasses and types are not modified
    environment['SECRET_VALUE'] = "1"
    C = resolve.make_configuration(SecretConfiguration())
    # auto derived type holds the value
    assert C.secret_value == "1"
    # base type is untouched
    assert SecretConfiguration.secret_value is None


def test_initial_values(environment: Any) -> None:
    # initial values will be overridden from env
    environment["PIPELINE_NAME"] = "env name"
    environment["CREATED_VAL"] = "12837"
    # set initial values and allow partial config
    C = resolve.make_configuration(CoercionTestConfiguration(),
        {"pipeline_name": "initial name", "none_val": type(environment), "created_val": 878232, "bytes_val": b"str"},
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
    C = resolve.make_configuration(WrongConfiguration(), accept_partial=True)
    assert C.NoneConfigVar is None
    # partial resolution
    assert not C.__is_resolved__
    assert C.is_partial()


def test_finds_version(environment: Any) -> None:
    global __version__

    v = __version__
    C = resolve.make_configuration(SimpleConfiguration())
    assert C._version == v
    try:
        del globals()["__version__"]
        C = resolve.make_configuration(SimpleConfiguration())
        assert not hasattr(C, "_version")
    finally:
        __version__ = v


def test_secret(environment: Any) -> None:
    with pytest.raises(ConfigEntryMissingException):
        resolve.make_configuration(SecretConfiguration())
    environment['SECRET_VALUE'] = "1"
    C = resolve.make_configuration(SecretConfiguration())
    assert C.secret_value == "1"
    # mock the path to point to secret storage
    # from dlt.common.configuration import config_utils
    path = environ_provider.SECRET_STORAGE_PATH
    del environment['SECRET_VALUE']
    try:
        # must read a secret file
        environ_provider.SECRET_STORAGE_PATH = "./tests/common/cases/%s"
        C = resolve.make_configuration(SecretConfiguration())
        assert C.secret_value == "BANANA"

        # set some weird path, no secret file at all
        del environment['SECRET_VALUE']
        environ_provider.SECRET_STORAGE_PATH = "!C:\\PATH%s"
        with pytest.raises(ConfigEntryMissingException):
            resolve.make_configuration(SecretConfiguration())

        # set env which is a fallback for secret not as file
        environment['SECRET_VALUE'] = "1"
        C = resolve.make_configuration(SecretConfiguration())
        assert C.secret_value == "1"
    finally:
        environ_provider.SECRET_STORAGE_PATH = path


def test_secret_kube_fallback(environment: Any) -> None:
    path = environ_provider.SECRET_STORAGE_PATH
    try:
        environ_provider.SECRET_STORAGE_PATH = "./tests/common/cases/%s"
        C = resolve.make_configuration(SecretKubeConfiguration())
        # all unix editors will add x10 at the end of file, it will be preserved
        assert C.secret_kube == "kube\n"
        # we propagate secrets back to environ and strip the whitespace
        assert environment['SECRET_KUBE'] == "kube"
    finally:
        environ_provider.SECRET_STORAGE_PATH = path


def test_coerce_values() -> None:
    with pytest.raises(ConfigEnvValueCannotBeCoercedException):
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
    with pytest.raises(ConfigEnvValueCannotBeCoercedException):
        coerce_single_value("key", "some string", LongInteger)
    with pytest.raises(ConfigEnvValueCannotBeCoercedException):
        coerce_single_value("key", "some string", Optional[LongInteger])  # type: ignore


def test_configuration_files(environment: Any) -> None:
    # overwrite config file paths
    environment["CONFIG_FILES_STORAGE_PATH"] = "./tests/common/cases/schemas/ev1/%s"
    C = resolve.make_configuration(MockProdConfigurationVar())
    assert C.config_files_storage_path == environment["CONFIG_FILES_STORAGE_PATH"]
    assert C.has_configuration_file("hasn't") is False
    assert C.has_configuration_file("event_schema.json") is True
    assert C.get_configuration_file_path("event_schema.json") == "./tests/common/cases/schemas/ev1/event_schema.json"
    with C.open_configuration_file("event_schema.json", "r") as f:
        f.read()
    with pytest.raises(ConfigFileNotFoundException):
        C.open_configuration_file("hasn't", "r")


def test_namespaced_configuration(environment: Any) -> None:
    with pytest.raises(ConfigEntryMissingException) as exc_val:
        resolve.make_configuration(NamespacedConfiguration())
    assert list(exc_val.value.traces.keys()) == ["password"]
    assert exc_val.value.spec_name == "NamespacedConfiguration"
    # check trace
    traces = exc_val.value.traces["password"]
    # only one provider and namespace was tried
    assert len(traces) == 1
    assert traces[0] == LookupTrace("Environment Variables", ("DLT_TEST",), "DLT_TEST__PASSWORD", None)

    # init vars work without namespace
    C = resolve.make_configuration(NamespacedConfiguration(), initial_value={"password": "PASS"})
    assert C.password == "PASS"

    # env var must be prefixed
    environment["PASSWORD"] = "PASS"
    with pytest.raises(ConfigEntryMissingException) as exc_val:
        resolve.make_configuration(NamespacedConfiguration())
    environment["DLT_TEST__PASSWORD"] = "PASS"
    C = resolve.make_configuration(NamespacedConfiguration())
    assert C.password == "PASS"


def coerce_single_value(key: str, value: str, hint: Type[Any]) -> Any:
    hint = extract_inner_type(hint)
    return resolve.deserialize_value(key, value, hint)
