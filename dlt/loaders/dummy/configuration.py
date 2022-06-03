from dlt.common.dataset_writers import TWriterType

class DummyClientConfiguration:
    WRITER_TYPE: TWriterType = "jsonl"
    FAIL_PROB: float = 0.0
    RETRY_PROB: float = 0.0
    COMPLETED_PROB: float = 0.0
    TIMEOUT: float = 10.0
