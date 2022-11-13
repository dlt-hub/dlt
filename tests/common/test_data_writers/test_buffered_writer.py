import pytest


@pytest.mark.skip("not implemented")
def test_insert_writer_schema_change() -> None:
    # split files if schema changes during write (only if something was dumped to storage)
    # add a flag to buffered writer: file split not allowed. prevent failed replaces
    pass