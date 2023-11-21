import dlt
import os
import time
import requests_mock

TRACE_URL_SUFFIX = "/trace"
STATE_URL_SUFFIX = "/state"

def test_platform_connection() -> None:

    mock_platform_url = "http://platform.com/endpoint"

    os.environ["RUNTIME__DLTHUB_DSN"] = mock_platform_url

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

    p = dlt.pipeline(destination="duckdb", pipeline_name="platform_test_pipeline", dataset_name="platform_test_dataset")

    with requests_mock.mock() as m:
        m.put(mock_platform_url, json={}, status_code=200)
        p.run([my_source(), my_source_2()])

        # sleep a bit and find trace in mock requests
        time.sleep(2)

        trace_result = None
        state_result = None
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
        assert len(trace_result["steps"]) == 4
        assert trace_result["execution_context"]["library"]["name"] == "dlt"

        # basic check of state result
        assert state_result, "no state update"
        assert state_result["pipeline_name"] == "platform_test_pipeline"
        assert state_result["dataset_name"] == "platform_test_dataset"
        assert len(state_result["schemas"]) == 2
        assert state_result["schemas"][0]["name"] == "first_source"
        assert state_result["schemas"][1]["name"] == "second_source"
