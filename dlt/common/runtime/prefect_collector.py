import time
import logging
from typing import Dict, Any, List, Union, TextIO

from dlt.common.runtime.collector import CallbackCollector
from dlt.common.exceptions import MissingDependencyException
from dlt.common import logger
try:
    import prefect
    from prefect import get_run_context
    from prefect.artifacts import create_markdown_artifact
except ModuleNotFoundError:
    raise MissingDependencyException("Prefect", ["prefect>=2.0"])


class PrefectCollector(CallbackCollector):
    """A Collector that creates Prefect artifacts for pipeline progress tracking.
    
    This collector extends CallbackCollector to create Prefect progress artifacts
    that show pipeline progress in the Prefect UI.
    """

    def __init__(
        self,
        log_period: float = 1.0,
        logger: Union[logging.Logger, TextIO] = None,
        log_level: int = logging.INFO,
        dump_system_stats: bool = True,
        create_artifacts: bool = True,
    ) -> None:
        """Initialize the Prefect collector.
        
        Args:
            log_period (float, optional): Time period in seconds between log updates. Defaults to 1.0.
            logger (logging.Logger | TextIO, optional): Logger or text stream to write log messages to. Defaults to None.
            log_level (str, optional): Log level for the logger. Defaults to INFO level
            dump_system_stats (bool, optional): Log memory and cpu usage. Defaults to True
            create_artifacts (bool, optional): Whether to create Prefect artifacts. Defaults to True
        """
        super().__init__(None, log_period, logger, log_level, dump_system_stats)
        self.create_artifacts = create_artifacts

    def on_log(self) -> None:
        """Called when LogCollector logs - creates Prefect progress artifacts."""
        if not self.create_artifacts or not self._prefect_available:
            return
            
        try:
            counters_summary = self._counters_to_summary()
            self._create_progress_artifact(counters_summary)
        except Exception as e:
            logger.warning(f"PrefectCollector artifact creation error: {e}")

    def _create_progress_artifact(self, counters_summary: Dict[str, Any]) -> None:
        """Create progress artifact based on current stage and counters."""
        try:
            context = get_run_context()
            
            # Determine stage and create appropriate progress artifact
            step = counters_summary.get("_step", "")
            
            if step.startswith("Extract"):
                self._create_extract_progress_artifact(counters_summary, context)
            elif step.startswith("Normalize"):
                self._create_normalize_progress_artifact(counters_summary, context)
            elif step.startswith("Load"):
                self._create_load_progress_artifact(counters_summary, context)
                
        except Exception as e:
            import logging
            logging.warning(f"Failed to create progress artifact: {e}")

    def _create_extract_progress_artifact(self, counters_summary: Dict[str, Any], context) -> None:
        """Create progress artifact for extract stage - rows added per table."""
        # TODO: Create table artifact showing rows added per table
        
        # For now, just create a simple progress artifact
        total_rows = 0
        table_counts = {}
        
        for counter_name, counter_data in counters_summary.items():
            if not counter_name.startswith("_") and counter_name not in ["Resources", "Files", "Items", "Jobs"]:
                # This is a table name during extract
                count = counter_data.get("count", 0)
                total_rows += count
                table_counts[counter_name] = count
        
        if total_rows > 0:
            markdown_content = f"""
# Extract Progress
**Total Rows:** {total_rows:,}

## Tables:
{chr(10).join([f"- **{table}**: {count:,} rows" for table, count in table_counts.items()])}
            """
            create_markdown_artifact(
                key=f"extract-progress-{int(time.time())}",
                markdown=markdown_content,
                description="Extract stage progress"
            )

    def _create_normalize_progress_artifact(self, counters_summary: Dict[str, Any], context) -> None:
        """Create progress artifact for normalize stage - files and items processed."""
        files_counter = counters_summary.get("Files", {})
        items_counter = counters_summary.get("Items", {})
        
        files_count = files_counter.get("count", 0)
        files_total = files_counter.get("total")
        items_count = items_counter.get("count", 0)
        
        if files_count > 0 or items_count > 0:
            
            progress_info = []
            if files_total:
                progress_info.append(f"**Files:** {files_count}/{files_total} ({files_count/files_total*100:.1f}%)")
            else:
                progress_info.append(f"**Files:** {files_count}")
            
            progress_info.append(f"**Items:** {items_count:,}")
            
            markdown_content = f"""
# Normalize Progress
{chr(10).join(progress_info)}

## Schema Updates:
TODO: Add schema update tracking
            """
            create_markdown_artifact(
                key=f"normalize-progress-{int(time.time())}",
                markdown=markdown_content,
                description="Normalize stage progress"
            )

    def _create_load_progress_artifact(self, counters_summary: Dict[str, Any], context) -> None:
        """Create progress artifact for load stage - jobs processed."""
        jobs_counter = counters_summary.get("Jobs", {})
        jobs_count = jobs_counter.get("count", 0)
        jobs_total = jobs_counter.get("total")
        
        if jobs_count > 0:
            
            if jobs_total:
                progress_text = f"**Jobs:** {jobs_count}/{jobs_total} ({jobs_count/jobs_total*100:.1f}%)"
            else:
                progress_text = f"**Jobs:** {jobs_count}"
            
            markdown_content = f"""
# Load Progress
{progress_text}

## Applied Updates:
TODO: Add applied update tracking
            """
            create_markdown_artifact(
                key=f"load-progress-{int(time.time())}",
                markdown=markdown_content,
                description="Load stage progress"
            )

    def _start(self, step: str) -> None:
        """Start tracking with Prefect task tags."""
        super()._start(step)
        
        # TODO: Add schema change detection logic
        # TODO: Append appropriate tags to task_run.tags
        
    def _stop(self) -> None:
        """Stop tracking and clean up Prefect task tags."""
        # TODO: Remove tags from task_run.tags
        
        super()._stop()

    def _update_task_tags(self, counters_summary: Dict[str, Any]) -> None:
        """Update task tags based on schema changes."""
        # TODO: Implement schema change detection
        # TODO: Append "Schema change" or "No schema change" tag
        pass

    def _get_schema_changes(self, counters_summary: Dict[str, Any]) -> Dict[str, List[str]]:
        """Extract schema changes from counter summary."""
        # TODO: Parse SchemaUpdates counter messages
        # TODO: Parse applied_update messages
        return {"tables": [], "columns": []}