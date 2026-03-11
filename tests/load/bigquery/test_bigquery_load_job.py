"""Tests for BigQueryLoadJob error handling, particularly internalError scenarios."""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from dlt.destinations.exceptions import (
    DatabaseTransientException,
    DatabaseTerminalException,
)
from dlt.destinations.impl.bigquery.bigquery import BigQueryLoadJob


# Test file path in correct format: table_name.file_id.retry_count.file_format
TEST_FILE_PATH = "/tmp/test_table.abc123.0.jsonl"


class TestBigQueryLoadJobInternalError:
    """Test cases for BigQueryLoadJob handling of BigQuery internalError."""

    def test_internal_error_raises_transient_exception(self):
        """When BigQuery returns internalError, a DatabaseTransientException should be raised.

        This tests the fix for the infinite loop bug where internalError would cause
        the job to loop forever checking the same failed state.
        """
        # Create a BigQueryLoadJob instance
        job = BigQueryLoadJob(
            file_path=TEST_FILE_PATH,
            http_timeout=15.0,
            retry_deadline=60.0,
        )

        # Mock the _bq_load_job
        mock_bq_job = MagicMock()
        mock_bq_job.done.return_value = True
        mock_bq_job.output_rows = None  # Indicates job didn't complete successfully
        mock_bq_job.error_result = {"reason": "internalError", "message": "Internal error occurred"}

        job._bq_load_job = mock_bq_job

        # Mock _job_client._create_load_job to return our mock job
        mock_client = MagicMock()
        mock_client._create_load_job.return_value = mock_bq_job
        job._job_client = mock_client

        # Mock _load_table
        job._load_table = {"name": "test_table"}

        # run() should raise DatabaseTransientException for internalError
        with pytest.raises(DatabaseTransientException) as exc_info:
            job.run()

        # Verify the exception message contains relevant info
        assert "internalError" in str(exc_info.value)
        assert "Internal error" in str(exc_info.value) or "internal error" in str(exc_info.value).lower()

    def test_terminal_error_raises_terminal_exception(self):
        """When BigQuery returns a terminal error like notFound, DatabaseTerminalException should be raised."""
        job = BigQueryLoadJob(
            file_path=TEST_FILE_PATH,
            http_timeout=15.0,
            retry_deadline=60.0,
        )

        mock_bq_job = MagicMock()
        mock_bq_job.done.return_value = True
        mock_bq_job.output_rows = None
        mock_bq_job.error_result = {"reason": "notFound", "message": "Table not found"}

        job._bq_load_job = mock_bq_job

        mock_client = MagicMock()
        mock_client._create_load_job.return_value = mock_bq_job
        job._job_client = mock_client
        job._load_table = {"name": "test_table"}

        with pytest.raises(DatabaseTerminalException) as exc_info:
            job.run()

        assert "notFound" in str(exc_info.value)

    def test_other_transient_error_raises_transient_exception(self):
        """When BigQuery returns other non-terminal errors, DatabaseTransientException should be raised."""
        job = BigQueryLoadJob(
            file_path=TEST_FILE_PATH,
            http_timeout=15.0,
            retry_deadline=60.0,
        )

        mock_bq_job = MagicMock()
        mock_bq_job.done.return_value = True
        mock_bq_job.output_rows = None
        mock_bq_job.error_result = {"reason": "backendError", "message": "Backend error"}

        job._bq_load_job = mock_bq_job

        mock_client = MagicMock()
        mock_client._create_load_job.return_value = mock_bq_job
        job._job_client = mock_client
        job._load_table = {"name": "test_table"}

        with pytest.raises(DatabaseTransientException) as exc_info:
            job.run()

        assert "backendError" in str(exc_info.value)

    def test_successful_job_completes_without_exception(self):
        """When BigQuery job succeeds, no exception should be raised."""
        job = BigQueryLoadJob(
            file_path=TEST_FILE_PATH,
            http_timeout=15.0,
            retry_deadline=60.0,
        )

        mock_bq_job = MagicMock()
        # First call returns False (not done), second call returns True (done)
        mock_bq_job.done.side_effect = [False, True]
        mock_bq_job.output_rows = 100  # Indicates successful completion
        mock_bq_job.error_result = None

        job._bq_load_job = mock_bq_job

        mock_client = MagicMock()
        mock_client._create_load_job.return_value = mock_bq_job
        job._job_client = mock_client
        job._load_table = {"name": "test_table"}

        # Patch sleep to avoid actual waiting
        with patch("dlt.common.runtime.signals.sleep"):
            # Should complete without exception
            job.run()

        # Verify the job was marked as created
        assert job._created_job is True

    def test_internal_error_exception_contains_error_details(self):
        """Verify that the DatabaseTransientException for internalError contains full error details."""
        job = BigQueryLoadJob(
            file_path=TEST_FILE_PATH,
            http_timeout=15.0,
            retry_deadline=60.0,
        )

        error_details = {
            "reason": "internalError",
            "message": "An internal error occurred during processing",
            "location": "us-central1",
        }
        mock_bq_job = MagicMock()
        mock_bq_job.done.return_value = True
        mock_bq_job.output_rows = None
        mock_bq_job.error_result = error_details

        job._bq_load_job = mock_bq_job

        mock_client = MagicMock()
        mock_client._create_load_job.return_value = mock_bq_job
        job._job_client = mock_client
        job._load_table = {"name": "test_table"}

        with pytest.raises(DatabaseTransientException) as exc_info:
            job.run()

        # The exception should contain the error details
        exception_str = str(exc_info.value)
        assert "internalError" in exception_str
        # Verify error_result dict is included in the message
        assert "reason" in exception_str or "Error details" in exception_str

