import pytest
from os import environ
from typing import Any, Dict, List, Mapping, MutableMapping, NewType, Optional, Tuple, Type

from dlt.common.typing import TSecretValue
from dlt.common.configuration import (
    RunConfiguration, ConfigEntryMissingException, ConfigFileNotFoundException,
    ConfigEnvValueCannotBeCoercedException, BaseConfiguration, utils, configspec)
from dlt.common.configuration.utils import make_configuration
from dlt.common.configuration.providers import environ as environ_provider

from tests.utils import preserve_environ

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
    'tuple_val': (1, 2, '7'),
    'set_val': {1, 2, 3},
    'bytes_val': b'Hello World!',
    'float_val': 1.18927,
    'any_val': "function() {}",
    'none_val': "none",
    'COMPLEX_VAL': {
        "_": (1440, ["*"], []),
        "change-email": (560, ["*"], [])
    }
}

INVALID_COERCIONS = {
    # 'STR_VAL': 'test string',  # string always OK
    'int_val': "a12345",
    'bool_val': "Yes",  # bool overridden by string - that is the most common problem
    'list_val': {1, "2", 3.0},
    'dict_val': "{'a': 1, 'b', '2'}",
    'tuple_val': [1, 2, '7'],
    'set_val': [1, 2, 3],
    'bytes_val': 'Hello World!',
    'float_val': "invalid"
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
    tuple_val: tuple = None  # type: ignore
    bytes_val: bytes = None
    set_val: set = None  # type: ignore
    float_val: float = None
    any_val: Any = None
    none_val: str = None
    COMPLEX_VAL: Dict[str, Tuple[int, List[str], List[str]]] = None


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


LongInteger = NewType("LongInteger", int)
FirstOrderStr = NewType("FirstOrderStr", str)
SecondOrderStr = NewType("SecondOrderStr", FirstOrderStr)


@pytest.fixture(scope="function")
def environment() -> Any:
    environ.clear()
    return environ


def test_run_configuration_gen_name(environment: Any) -> None:
    C = make_configuration(RunConfiguration())
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
    C = make_configuration(SecretConfiguration())
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
    assert hasattr(C, "__is_partial__")
    assert hasattr(C, "__namespace__")

    with pytest.raises(KeyError):
        C["_version"]

    # set ops
    # update supported and non existing attributes are ignored
    C.update({"pipeline_name": "old pipe", "__version": "1.1.1"})
    assert C.pipeline_name == "old pipe" == C["pipeline_name"]
    assert C._version != "1.1.1"

    # delete is not supported
    with pytest.raises(NotImplementedError):
        del C["pipeline_name"]

    with pytest.raises(NotImplementedError):
        C.pop("pipeline_name", None)

    # setting supported
    C["pipeline_name"] = "new pipe"
    assert C.pipeline_name == "new pipe" == C["pipeline_name"]

    with pytest.raises(KeyError):
        C["_version"] = "1.1.1"


def test_fields_with_no_default_to_null() -> None:
    # fields with no default are promoted to class attrs with none
    assert FieldWithNoDefaultConfiguration.no_default is None
    assert FieldWithNoDefaultConfiguration().no_default is None


def test_init_method_gen() -> None:
    C = FieldWithNoDefaultConfiguration(no_default="no_default", sentry_dsn="SENTRY")
    assert C.no_default == "no_default"
    assert C.sentry_dsn == "SENTRY"


def test_multi_derivation_defaults() -> None:

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


def test_raises_on_unresolved_fields() -> None:
    with pytest.raises(ConfigEntryMissingException) as config_entry_missing_exception:
        C = WrongConfiguration()
        keys = utils._get_resolvable_fields(C)
        utils._is_config_bounded(C, keys)

    assert 'NONECONFIGVAR' in config_entry_missing_exception.value.missing_set

    # via make configuration
    with pytest.raises(ConfigEntryMissingException) as config_entry_missing_exception:
        make_configuration(WrongConfiguration())
    assert 'NONECONFIGVAR' in config_entry_missing_exception.value.missing_set


def test_optional_types_are_not_required() -> None:
    # this should not raise an exception
    keys = utils._get_resolvable_fields(ConfigurationWithOptionalTypes())
    utils._is_config_bounded(ConfigurationWithOptionalTypes(), keys)
    # make optional config
    make_configuration(ConfigurationWithOptionalTypes())
    # make config with optional values
    make_configuration(ProdConfigurationWithOptionalTypes(), initial_value={"INT_VAL": None})


def test_configuration_apply_adds_environment_variable_to_config(environment: Any) -> None:
    environment["NONECONFIGVAR"] = "Some"

    C = WrongConfiguration()
    keys = utils._get_resolvable_fields(C)
    utils._resolve_config_fields(C, keys, accept_partial=False)
    utils._is_config_bounded(C, keys)

    assert C.NoneConfigVar == environment["NONECONFIGVAR"]


def test_configuration_resolve_env_var(environment: Any) -> None:
    environment["TEST_BOOL"] = 'True'

    C = SimpleConfiguration()
    keys = utils._get_resolvable_fields(C)
    utils._resolve_config_fields(C, keys, accept_partial=False)
    utils._is_config_bounded(C, keys)

    # value will be coerced to bool
    assert C.test_bool is True


def test_find_all_keys() -> None:
    keys = utils._get_resolvable_fields(VeryWrongConfiguration())
    # assert hints and types: LOG_COLOR had it hint overwritten in derived class
    assert set({'str_val': str, 'int_val': int, 'NoneConfigVar': str, 'log_color': str}.items()).issubset(keys.items())


def test_coercions(environment: Any) -> None:
    for key, value in COERCIONS.items():
        environment[key.upper()] = str(value)

    C = CoercionTestConfiguration()
    keys = utils._get_resolvable_fields(C)
    utils._resolve_config_fields(C, keys, accept_partial=False)
    utils._is_config_bounded(C, keys)

    for key in COERCIONS:
        assert getattr(C, key) == COERCIONS[key]


def test_invalid_coercions(environment: Any) -> None:
    C = CoercionTestConfiguration()
    config_keys = utils._get_resolvable_fields(C)
    for key, value in INVALID_COERCIONS.items():
        try:
            environment[key.upper()] = str(value)
            utils._resolve_config_fields(C, config_keys, accept_partial=False)
        except ConfigEnvValueCannotBeCoercedException as coerc_exc:
            # must fail exactly on expected value
            if coerc_exc.attr_name != key:
                raise
            # overwrite with valid value and go to next env
            environment[key.upper()] = str(COERCIONS[key])
            continue
        raise AssertionError("%s was coerced with %s which is invalid type" % (key, value))


def test_excepted_coercions(environment: Any) -> None:
    C = CoercionTestConfiguration()
    config_keys = utils._get_resolvable_fields(C)
    for k, v in EXCEPTED_COERCIONS.items():
        environment[k.upper()] = str(v)
        utils._resolve_config_fields(C, config_keys, accept_partial=False)
    for key in EXCEPTED_COERCIONS:
        assert getattr(C, key) == COERCED_EXCEPTIONS[key]


def test_make_configuration(environment: Any) -> None:
    # fill up configuration
    environment["NONECONFIGVAR"] = "1"
    C = utils.make_configuration(WrongConfiguration())
    assert not C.__is_partial__
    assert C.NoneConfigVar == "1"


def test_auto_derivation(environment: Any) -> None:
    # make_configuration works on instances of dataclasses and types are not modified
    environment['SECRET_VALUE'] = "1"
    C = utils.make_configuration(SecretConfiguration())
    # auto derived type holds the value
    assert C.secret_value == "1"
    # base type is untouched
    assert SecretConfiguration.secret_value is None


def test_initial_values(environment: Any) -> None:
    # initial values will be overridden from env
    environment["PIPELINE_NAME"] = "env name"
    environment["CREATED_VAL"] = "12837"
    # set initial values and allow partial config
    C = make_configuration(CoercionTestConfiguration(),
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
    C = make_configuration(WrongConfiguration(), accept_partial=True)
    assert C.NoneConfigVar is None
    # partial resolution
    assert C.__is_partial__


def test_finds_version(environment: Any) -> None:
    global __version__

    v = __version__
    C = utils.make_configuration(SimpleConfiguration())
    assert C._version == v
    try:
        del globals()["__version__"]
        C = utils.make_configuration(SimpleConfiguration())
        assert not hasattr(C, "_version")
    finally:
        __version__ = v


def test_secret(environment: Any) -> None:
    with pytest.raises(ConfigEntryMissingException):
        utils.make_configuration(SecretConfiguration())
    environment['SECRET_VALUE'] = "1"
    C = utils.make_configuration(SecretConfiguration())
    assert C.secret_value == "1"
    # mock the path to point to secret storage
    # from dlt.common.configuration import config_utils
    path = environ_provider.SECRET_STORAGE_PATH
    del environment['SECRET_VALUE']
    try:
        # must read a secret file
        environ_provider.SECRET_STORAGE_PATH = "./tests/common/cases/%s"
        C = utils.make_configuration(SecretConfiguration())
        assert C.secret_value == "BANANA"

        # set some weird path, no secret file at all
        del environment['SECRET_VALUE']
        environ_provider.SECRET_STORAGE_PATH = "!C:\\PATH%s"
        with pytest.raises(ConfigEntryMissingException):
            utils.make_configuration(SecretConfiguration())

        # set env which is a fallback for secret not as file
        environment['SECRET_VALUE'] = "1"
        C = utils.make_configuration(SecretConfiguration())
        assert C.secret_value == "1"
    finally:
        environ_provider.SECRET_STORAGE_PATH = path


def test_secret_kube_fallback(environment: Any) -> None:
    path = environ_provider.SECRET_STORAGE_PATH
    try:
        environ_provider.SECRET_STORAGE_PATH = "./tests/common/cases/%s"
        C = utils.make_configuration(SecretKubeConfiguration())
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
    C = utils.make_configuration(MockProdConfigurationVar())
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
        utils.make_configuration(NamespacedConfiguration())
    assert exc_val.value.missing_set == ["DLT_TEST__PASSWORD"]
    assert exc_val.value.namespace == "DLT_TEST"

    # init vars work without namespace
    C = utils.make_configuration(NamespacedConfiguration(), initial_value={"password": "PASS"})
    assert C.password == "PASS"

    # env var must be prefixed
    environment["PASSWORD"] = "PASS"
    with pytest.raises(ConfigEntryMissingException) as exc_val:
        utils.make_configuration(NamespacedConfiguration())
    environment["DLT_TEST__PASSWORD"] = "PASS"
    C = utils.make_configuration(NamespacedConfiguration())
    assert C.password == "PASS"

def coerce_single_value(key: str, value: str, hint: Type[Any]) -> Any:
    hint = utils._extract_simple_type(hint)
    return utils._coerce_single_value(key, value, hint)
