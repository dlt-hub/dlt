import dlt
import os
import time
import requests_mock

def test_platform_connection() -> None:

    mock_platform_url = "http://platform.com/endpoint"

    os.environ["RUNTIME__PLATFORM_DSN"] = mock_platform_url

    # simple pipeline
    @dlt.source(name="platform_test")
    def my_source():

        @dlt.resource(name="test_resource")
        def data():
            yield [1, 2, 3]

        return data()

    p = dlt.pipeline(destination="duckdb", pipeline_name="platform_test_pipeline")

    with requests_mock.mock() as m:
        m.put(mock_platform_url, json={}, status_code=200)
        p.run(my_source())

        # sleep a bit and find trace in mock requests
        time.sleep(1)

        trace_result = None
        for call in m.request_history:
            if call.url == mock_platform_url:
                assert not trace_result, "Multiple calls to platform dsn endpoint"
                trace_result = call.json()

        # basic check of trace result
        assert trace_result
        assert trace_result["pipeline_name"] == "platform_test_pipeline"
        assert len(trace_result["steps"]) == 4
        assert trace_result["execution_context"]["library"]["name"] == "dlt"
