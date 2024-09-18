import os
import pytest
import requests_mock

import dlt

from tests.utils import start_test_telemetry, stop_telemetry

TRACE_URL_SUFFIX = "/trace"
STATE_URL_SUFFIX = "/state"


@pytest.mark.forked
def test_platform_connection() -> None:
    mock_platform_url = "http://platform.com/endpoint"
    os.environ["RUNTIME__DLTHUB_DSN"] = mock_platform_url
    start_test_telemetry()

    trace_url = mock_platform_url + TRACE_URL_SUFFIX
    state_url = mock_platform_url + STATE_URL_SUFFIX

    # simple pipeline
    @dlt.source(name="first_source")
    def my_source():
        @dlt.resource(name="test_resource")
        def data():
            yield [1, 2, 3]

        return data()

    @dlt.source(name="second_source")
    def my_source_2():
        @dlt.resource(name="test_resource")
        def data():
            yield [1, 2, 3]

        return data()

    p = dlt.pipeline(
        destination="duckdb",
        pipeline_name="platform_test_pipeline",
        dataset_name="platform_test_dataset",
    )

    with requests_mock.mock() as m:
        m.put(mock_platform_url, json={}, status_code=200)
        p.run([my_source(), my_source_2()])

        # this will flush all telemetry queues
        stop_telemetry()

        trace_result = None
        state_result = None

        # mock tracks all calls
        for call in m.request_history:
            if call.url == trace_url:
                assert not trace_result, "Multiple calls to trace endpoint"
                trace_result = call.json()

            if call.url == state_url:
                assert not state_result, "Multiple calls to state endpoint"
                state_result = call.json()

        # basic check of trace result
        assert trace_result, "no trace"
        assert trace_result["pipeline_name"] == "platform_test_pipeline"
        # just extract, normalize and load steps. run step is not serialized to trace (it was just a copy of load)
        assert len(trace_result["steps"]) == 3
        assert trace_result["execution_context"]["library"]["name"] == "dlt"

        # basic check of state result
        assert state_result, "no state update"
        assert state_result["pipeline_name"] == "platform_test_pipeline"
        assert state_result["dataset_name"] == "platform_test_dataset"
        assert len(state_result["schemas"]) == 2
        assert {state_result["schemas"][0]["name"], state_result["schemas"][1]["name"]} == {
            "first_source",
            "second_source",
        }
