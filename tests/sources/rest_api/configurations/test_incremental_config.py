import re
import dlt.common
import dlt.common.exceptions
from dlt.common import pendulum

import dlt.extract
import pytest
from typing import cast


import dlt

from dlt.extract.incremental import Incremental

from dlt.sources.rest_api import (
    _validate_param_type,
    _set_incremental_params,
)

from dlt.sources.rest_api.config_setup import (
    IncrementalParam,
    setup_incremental_object,
)
from dlt.sources.rest_api.typing import (
    IncrementalConfig,
)

try:
    from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
except ImportError:
    pass


@pytest.fixture()
def incremental_with_init_and_end() -> Incremental[str]:
    return dlt.sources.incremental(
        cursor_path="updated_at",
        initial_value="2024-01-01T00:00:00Z",
        end_value="2024-06-30T00:00:00Z",
    )


@pytest.fixture()
def incremental_with_init() -> Incremental[str]:
    return dlt.sources.incremental(
        cursor_path="updated_at",
        initial_value="2024-01-01T00:00:00Z",
    )


def test_invalid_incremental_type_is_not_accepted() -> None:
    request_params = {
        "foo": "bar",
        "since": {
            "type": "no_incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
        },
    }
    with pytest.raises(ValueError) as e:
        _validate_param_type(request_params)

    assert e.match("Invalid param type: no_incremental.")


def test_one_resource_cannot_have_many_incrementals() -> None:
    request_params = {
        "foo": "bar",
        "first_incremental": {
            "type": "incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
        },
        "second_incremental": {
            "type": "incremental",
            "cursor_path": "created_at",
            "initial_value": "2024-01-01T00:00:00Z",
        },
    }
    with pytest.raises(ValueError) as e:
        setup_incremental_object(request_params)
    error_message = re.escape(
        "Only a single incremental parameter is allower per endpoint. Found: ['first_incremental',"
        " 'second_incremental']"
    )
    assert e.match(error_message)


def test_one_resource_cannot_have_many_incrementals_2(incremental_with_init) -> None:
    request_params = {
        "foo": "bar",
        "first_incremental": {
            "type": "incremental",
            "cursor_path": "created_at",
            "initial_value": "2024-02-02T00:00:00Z",
        },
        "second_incremental": incremental_with_init,
    }
    with pytest.raises(ValueError) as e:
        setup_incremental_object(request_params)
    error_message = re.escape(
        "Only a single incremental parameter is allower per endpoint. Found: ['first_incremental',"
        " 'second_incremental']"
    )
    assert e.match(error_message)


def test_constructs_incremental_from_request_param() -> None:
    request_params = {
        "foo": "bar",
        "since": {
            "type": "incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
        },
    }
    (incremental_config, incremental_param, _) = setup_incremental_object(request_params)
    assert incremental_config == dlt.sources.incremental(
        cursor_path="updated_at", initial_value="2024-01-01T00:00:00Z"
    )
    assert incremental_param == IncrementalParam(start="since", end=None)


def test_constructs_incremental_from_request_param_with_incremental_object(
    incremental_with_init,
) -> None:
    request_params = {
        "foo": "bar",
        "since": dlt.sources.incremental(
            cursor_path="updated_at", initial_value="2024-01-01T00:00:00Z"
        ),
    }
    (incremental_obj, incremental_param, _) = setup_incremental_object(request_params)
    assert incremental_param == IncrementalParam(start="since", end=None)

    assert incremental_with_init == incremental_obj


def test_constructs_incremental_from_request_param_with_convert(
    incremental_with_init,
) -> None:
    def epoch_to_datetime(epoch: str):
        return pendulum.from_timestamp(int(epoch))

    param_config = {
        "since": {
            "type": "incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
            "convert": epoch_to_datetime,
        }
    }

    (incremental_obj, incremental_param, convert) = setup_incremental_object(param_config, None)
    assert incremental_param == IncrementalParam(start="since", end=None)
    assert convert == epoch_to_datetime

    assert incremental_with_init == incremental_obj


def test_does_not_construct_incremental_from_request_param_with_unsupported_incremental(
    incremental_with_init_and_end,
) -> None:
    param_config = {
        "since": {
            "type": "incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
            "end_value": "2024-06-30T00:00:00Z",  # This is ignored
        }
    }

    with pytest.raises(ValueError) as e:
        setup_incremental_object(param_config)

    assert e.match(
        "Only start_param and initial_value are allowed in the configuration of param: since."
    )

    param_config_2 = {
        "since_2": {
            "type": "incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
            "end_param": "2024-06-30T00:00:00Z",  # This is ignored
        }
    }

    with pytest.raises(ValueError) as e:
        setup_incremental_object(param_config_2)

    assert e.match(
        "Only start_param and initial_value are allowed in the configuration of param: since_2."
    )

    param_config_3 = {"since_3": incremental_with_init_and_end}

    with pytest.raises(ValueError) as e:
        setup_incremental_object(param_config_3)

    assert e.match("Only initial_value is allowed in the configuration of param: since_3.")


def test_constructs_incremental_from_endpoint_config_incremental(
    incremental_with_init,
) -> None:
    config = {
        "incremental": {
            "start_param": "since",
            "end_param": "until",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
        }
    }
    incremental_config = cast(IncrementalConfig, config.get("incremental"))
    (incremental_obj, incremental_param, _) = setup_incremental_object(
        {},
        incremental_config,
    )
    assert incremental_param == IncrementalParam(start="since", end="until")

    assert incremental_with_init == incremental_obj


def test_constructs_incremental_from_endpoint_config_incremental_with_convert(
    incremental_with_init_and_end,
) -> None:
    def epoch_to_datetime(epoch):
        return pendulum.from_timestamp(int(epoch))

    resource_config_incremental: IncrementalConfig = {
        "start_param": "since",
        "end_param": "until",
        "cursor_path": "updated_at",
        "initial_value": "2024-01-01T00:00:00Z",
        "end_value": "2024-06-30T00:00:00Z",
        "convert": epoch_to_datetime,
    }

    (incremental_obj, incremental_param, convert) = setup_incremental_object(
        {}, resource_config_incremental
    )
    assert incremental_param == IncrementalParam(start="since", end="until")
    assert convert == epoch_to_datetime
    assert incremental_with_init_and_end == incremental_obj


def test_calls_convert_from_endpoint_config_incremental(mocker) -> None:
    def epoch_to_date(epoch: str):
        return pendulum.from_timestamp(int(epoch)).to_date_string()

    callback = mocker.Mock(side_effect=epoch_to_date)
    incremental_obj = mocker.Mock()
    incremental_obj.last_value = "1"

    incremental_param = IncrementalParam(start="since", end=None)
    created_param = _set_incremental_params({}, incremental_obj, incremental_param, callback)
    assert created_param == {"since": "1970-01-01"}
    assert callback.call_args_list[0].args == ("1",)


def test_calls_convert_from_request_param(mocker) -> None:
    def epoch_to_datetime(epoch: str):
        return pendulum.from_timestamp(int(epoch)).to_date_string()

    callback = mocker.Mock(side_effect=epoch_to_datetime)
    start = 1
    one_day_later = 60 * 60 * 24
    incremental_config: IncrementalConfig = {
        "start_param": "since",
        "end_param": "until",
        "cursor_path": "updated_at",
        "initial_value": str(start),
        "end_value": str(one_day_later),
        "convert": callback,
    }

    (incremental_obj, incremental_param, _) = setup_incremental_object({}, incremental_config)
    assert incremental_param is not None
    assert incremental_obj is not None
    created_param = _set_incremental_params({}, incremental_obj, incremental_param, callback)
    assert created_param == {"since": "1970-01-01", "until": "1970-01-02"}
    assert callback.call_args_list[0].args == (str(start),)
    assert callback.call_args_list[1].args == (str(one_day_later),)


def test_default_convert_is_identity() -> None:
    start = 1
    one_day_later = 60 * 60 * 24
    incremental_config: IncrementalConfig = {
        "start_param": "since",
        "end_param": "until",
        "cursor_path": "updated_at",
        "initial_value": str(start),
        "end_value": str(one_day_later),
    }

    (incremental_obj, incremental_param, _) = setup_incremental_object({}, incremental_config)
    assert incremental_param is not None
    assert incremental_obj is not None
    created_param = _set_incremental_params({}, incremental_obj, incremental_param, None)
    assert created_param == {"since": str(start), "until": str(one_day_later)}


def test_incremental_param_transform_is_deprecated(incremental_with_init) -> None:
    """Tests that deprecated interface works but issues deprecation warning"""

    def epoch_to_datetime(epoch: str):
        return pendulum.from_timestamp(int(epoch))

    param_config = {
        "since": {
            "type": "incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
            "transform": epoch_to_datetime,
        }
    }

    with pytest.deprecated_call():
        (incremental_obj, incremental_param, convert) = setup_incremental_object(param_config, None)

        assert incremental_param == IncrementalParam(start="since", end=None)
        assert convert == epoch_to_datetime

        assert incremental_with_init == incremental_obj


def test_incremental_endpoint_config_transform_is_deprecated(
    incremental_with_init_and_end,
) -> None:
    """Tests that deprecated interface works but issues deprecation warning"""

    def epoch_to_datetime(epoch):
        return pendulum.from_timestamp(int(epoch))

    resource_config_incremental: IncrementalConfig = {
        "start_param": "since",
        "end_param": "until",
        "cursor_path": "updated_at",
        "initial_value": "2024-01-01T00:00:00Z",
        "end_value": "2024-06-30T00:00:00Z",
        "transform": epoch_to_datetime,  # type: ignore[typeddict-unknown-key]
    }

    with pytest.deprecated_call():
        (incremental_obj, incremental_param, convert) = setup_incremental_object(
            {}, resource_config_incremental
        )
        assert incremental_param == IncrementalParam(start="since", end="until")
        assert convert == epoch_to_datetime
        assert incremental_with_init_and_end == incremental_obj
