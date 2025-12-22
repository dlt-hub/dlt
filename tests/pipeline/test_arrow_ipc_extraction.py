import os
import pytest
import pyarrow as pa
import dlt
from dlt.common.utils import uniq_id
from tests.utils import clean_test_storage

@pytest.fixture
def arrow_table():
    return pa.Table.from_pydict({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

def test_extract_arrow_default_parquet(arrow_table):
    """Verify that by default arrow data is extracted as parquet"""
    pipeline = dlt.pipeline("arrow_default_" + uniq_id(), destination="dummy")

    @dlt.resource(name="arrow_data")
    def some_data():
        yield arrow_table

    # Run extract only
    pipeline.extract(some_data())
    
    # Check extracted files
    norm_storage = pipeline._get_normalize_storage()
    extract_files = [f for f in norm_storage.list_files_to_normalize_sorted() if not f.split("/")[-1].startswith("_dlt_pipeline_state")]
    
    assert len(extract_files) == 1
    file_path = extract_files[0]
    
    # Should be parquet extension
    assert file_path.endswith(".parquet")
    
    # Verify content is parquet
    with norm_storage.extracted_packages.storage.open_file(file_path, "rb") as f:
        # Parquet magic bytes
        magic = f.read(4)
        assert magic == b"PAR1"

def test_extract_arrow_ipc_hint(arrow_table):
    """Verify that with file_format='arrow' hint, data is extracted as Arrow IPC"""
    pipeline = dlt.pipeline("arrow_ipc_" + uniq_id(), destination="dummy")

    @dlt.resource(name="arrow_data", file_format="arrow")
    def some_data():
        yield arrow_table

    # Run extract only
    pipeline.extract(some_data())
    
    # Check extracted files
    norm_storage = pipeline._get_normalize_storage()
    extract_files = [f for f in norm_storage.list_files_to_normalize_sorted() if not f.split("/")[-1].startswith("_dlt_pipeline_state")]
    
    assert len(extract_files) == 1
    file_path = extract_files[0]
    
    # Should be arrow extension
    assert file_path.endswith(".arrow")
    
    # Verify content is Arrow IPC
    with norm_storage.extracted_packages.storage.open_file(file_path, "rb") as f:
        # Arrow IPC stream format usually starts with specific bytes, 
        # but easiest is to try reading it with pyarrow
        f.seek(0)
        reader = pa.ipc.open_file(f)
        table = reader.read_all()
        assert table.equals(arrow_table)

def test_extract_arrow_ipc_hint_mixed_with_parquet(arrow_table):
    """Verify that we can have mixed resources: one default (parquet) and one explicit arrow ipc"""
    pipeline = dlt.pipeline("arrow_mixed_" + uniq_id(), destination="dummy")

    @dlt.resource(name="parquet_data")
    def parquet_data():
        yield arrow_table

    @dlt.resource(name="ipc_data", file_format="arrow")
    def ipc_data():
        yield arrow_table

    # Run extract only
    pipeline.extract([parquet_data(), ipc_data()])
    
    # Check extracted files
    norm_storage = pipeline._get_normalize_storage()
    extract_files = [f for f in norm_storage.list_files_to_normalize_sorted() if not f.split("/")[-1].startswith("_dlt_pipeline_state")]
    
    assert len(extract_files) == 2
    
    parquet_files = [f for f in extract_files if f.endswith(".parquet")]
    arrow_files = [f for f in extract_files if f.endswith(".arrow")]
    
    assert len(parquet_files) == 1
    assert len(arrow_files) == 1
    
    # Verify parquet file
    with norm_storage.extracted_packages.storage.open_file(parquet_files[0], "rb") as f:
        assert f.read(4) == b"PAR1"
        
    # Verify arrow file
    with norm_storage.extracted_packages.storage.open_file(arrow_files[0], "rb") as f:
        reader = pa.ipc.open_file(f)
        table = reader.read_all()
        assert table.equals(arrow_table)
