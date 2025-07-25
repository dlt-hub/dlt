import time
import logging
from typing import Any, Dict, List, Union, TextIO

from dlt.common.runtime.collector import LogCollector
from dlt.common.exceptions import MissingDependencyException
from dlt.common import logger
from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace
from dlt.common.pipeline import SupportsPipeline
from dlt.common.runtime.exec_info import get_execution_context

try:
    import prefect
    from prefect.context import get_run_context, TaskRunContext, FlowRunContext
    from prefect.artifacts import (
        create_markdown_artifact,
        create_progress_artifact,
        update_progress_artifact,
    )
except ModuleNotFoundError:
    raise MissingDependencyException("Prefect", ["prefect>=2.0"])


class PrefectCollector(LogCollector):
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
        super().__init__(log_period, logger, log_level, dump_system_stats)
        self.create_artifacts = create_artifacts
        self.progress_artifact_ids = {}  # Dict to store progress artifact IDs for each stage
        self.pipeline_instance = None  # Store the pipeline instance for artifact creation

    def _counter_to_markdown_line(
        self, counter_key: str, count: int, info, current_time: float
    ) -> str:
        """Convert a single counter to a markdown line format.

        Args:
            counter_key: The counter key
            count: Current count value
            info: CounterInfo object
            current_time: Current timestamp

        Returns:
            str: Formatted markdown line for this counter
        """
        # Reuse the log line method and add markdown formatting
        log_line = self._counter_to_log_line(counter_key, count, info, current_time)

        # Convert to markdown format: add bullet point and bold the description
        # The log line format is: "description: progress percentage | Time: elapsed_time | Rate: items_per_second message"
        # We want: "- **description**: progress percentage | Time: elapsed_time | Rate: items_per_second *message*"

        # Split on first colon to separate description from the rest
        if ": " in log_line:
            description, rest = log_line.split(": ", 1)
            # Bold the description and add bullet point
            return f"- **{description}**: {rest}"
        else:
            # Fallback if format is unexpected
            return f"- {log_line}"

    def _system_stats_to_markdown_line(self) -> str:
        """Convert system stats to a markdown line format."""
        # Reuse the log line method and add markdown formatting
        log_line = self._system_stats_to_log_line()

        # Convert to markdown format: add bullet point and bold the labels
        # The log line format is: "Memory usage: X MB (Y%) | CPU usage: Z%"
        # We want: "- **Memory**: X MB (Y%) | **CPU**: Z%"

        # Replace "Memory usage:" with "**Memory**:" and "CPU usage:" with "**CPU**:"
        markdown_line = log_line.replace("Memory usage:", "**Memory**:").replace(
            "CPU usage:", "**CPU**:"
        )

        # Add bullet point
        return f"- {markdown_line}"

    def _add_pipeline_info_to_markdown(self, markdown_lines: List[str], pipeline: SupportsPipeline) -> None:
        """Add pipeline and execution context information to markdown lines.
        
        Args:
            markdown_lines: List of markdown lines to append to
            pipeline: The pipeline object to get information from
        """
        markdown_lines.append("")
        markdown_lines.append("## Pipeline Information:")
        
        # Pipeline basic info
        markdown_lines.append(f"- **Pipeline Name**: {pipeline.pipeline_name}")
        markdown_lines.append(f"- **Working Directory**: {pipeline.working_dir}")
        markdown_lines.append(f"- **First Run**: {pipeline.first_run}")
        markdown_lines.append(f"- **Default Schema**: {pipeline.default_schema_name}")
        
        # Destination info from pipeline state
        state = pipeline.state
        if state:
            markdown_lines.append(f"- **Destination Name**: {state.get('destination_name', 'N/A')}")
            markdown_lines.append(f"- **Destination Type**: {state.get('destination_type', 'N/A')}")
            markdown_lines.append(f"- **Staging Name**: {state.get('staging_name', 'N/A')}")
            markdown_lines.append(f"- **Staging Type**: {state.get('staging_type', 'N/A')}")
        
        # Execution context info
        exec_context = get_execution_context()
        markdown_lines.append("")
        markdown_lines.append("## Execution Context:")
        markdown_lines.append(f"- **Python Version**: {exec_context.get('python', 'N/A')}")
        markdown_lines.append(f"- **OS**: {exec_context.get('os', {}).get('name', 'N/A')} {exec_context.get('os', {}).get('version', '')}")
        markdown_lines.append(f"- **Library Version**: {exec_context.get('library', {}).get('name', 'N/A')} {exec_context.get('library', {}).get('version', '')}")
        markdown_lines.append(f"- **CPU Cores**: {exec_context.get('cpu', 'N/A')}")
        
        # Execution environment info
        exec_info = exec_context.get('exec_info', [])
        if exec_info:
            markdown_lines.append(f"- **Execution Environment**: {', '.join(exec_info)}")

    def _create_stage_summary_markdown_artifact(self, pipeline: SupportsPipeline) -> None:
        """Create markdown artifact based on current stage and counters."""
        if not self.create_artifacts:
            return

        try:
            # Determine stage and create appropriate markdown artifact
            step = self.step

            # Skip composite steps (run, sync) as they don't need their own artifacts
            if step.startswith("run") or step.startswith("sync"):
                return

            if step.startswith("Extract"):
                self._create_extract_stage_summary_markdown_artifact(pipeline)
            elif step.startswith("Normalize"):
                self._create_normalize_stage_summary_markdown_artifact(pipeline)
            elif step.startswith("Load"):
                self._create_load_stage_summary_markdown_artifact(pipeline)

        except Exception as e:
            import logging
            import traceback
            logging.warning(f"Failed to create markdown artifact: {e}")
            logging.warning(f"Traceback: {traceback.format_exc()}")

    def _create_extract_stage_summary_markdown_artifact(self, pipeline: SupportsPipeline) -> None:
        """Create markdown artifact for extract stage - rows added per table."""
        # Use shared formatting methods to create detailed markdown
        current_time = time.time()
        markdown_lines = []

        # Add stage header
        markdown_lines.append(f"# {self.step} Stage Summary")
        markdown_lines.append("")

        # Add counter details using shared formatting
        total_rows = 0
        table_counts = {}


        for counter_key, count in self.counters.items():
            info = self.counter_info.get(counter_key)
            if info:
                # Use shared markdown formatting
                markdown_lines.append(
                    self._counter_to_markdown_line(counter_key, count, info, current_time)
                )

                # Track table counts for summary
                if not counter_key.startswith("_") and counter_key not in [
                    "Resources",
                    "Files",
                    "Items",
                    "Jobs",
                ]:
                    total_rows += count
                    table_counts[counter_key] = count

        # Add summary section
        if total_rows > 0:
            markdown_lines.append("")
            markdown_lines.append("## Summary:")
            markdown_lines.append(f"- **Total Rows**: {total_rows:,}")
            for table, count in table_counts.items():
                markdown_lines.append(f"- Table **{table}**: {count:,} rows")

        # Add system stats if enabled
        if self.dump_system_stats:
            markdown_lines.append("")
            markdown_lines.append("## System Stats:")
            markdown_lines.append(self._system_stats_to_markdown_line())

        # Add pipeline and execution context info
        self._add_pipeline_info_to_markdown(markdown_lines, pipeline)

        markdown_content = "\n".join(markdown_lines)
        create_markdown_artifact(
            key=f"extract-summary-{int(time.time())}",
            markdown=markdown_content,
            description="Extract stage summary",
        )

    def _create_normalize_stage_summary_markdown_artifact(self, pipeline: SupportsPipeline) -> None:
        """Create markdown artifact for normalize stage - files and items processed."""
        # Use shared formatting methods to create detailed markdown
        current_time = time.time()
        markdown_lines = []

        # Add stage header
        markdown_lines.append(f"# {self.step} Stage Summary")
        markdown_lines.append("")

        # Add counter details using shared formatting
        for counter_key, count in self.counters.items():
            info = self.counter_info.get(counter_key)
            if info:
                # Use shared markdown formatting
                markdown_lines.append(
                    self._counter_to_markdown_line(counter_key, count, info, current_time)
                )

        # Add system stats if enabled
        if self.dump_system_stats:
            markdown_lines.append("")
            markdown_lines.append("## System Stats:")
            markdown_lines.append(self._system_stats_to_markdown_line())

        # Add pipeline and execution context info
        self._add_pipeline_info_to_markdown(markdown_lines, pipeline)

        markdown_content = "\n".join(markdown_lines)
        create_markdown_artifact(
            key=f"normalize-summary-{int(time.time())}",
            markdown=markdown_content,
            description="Normalize stage summary",
        )

    def _create_load_stage_summary_markdown_artifact(self, pipeline: SupportsPipeline) -> None:
        """Create markdown artifact for load stage - jobs processed."""
        # Use shared formatting methods to create detailed markdown
        current_time = time.time()
        markdown_lines = []

        # Add stage header
        markdown_lines.append(f"# {self.step} Stage Summary")
        markdown_lines.append("")

        # Add counter details using shared formatting
        for counter_key, count in self.counters.items():
            info = self.counter_info.get(counter_key)
            if info:
                # Use shared markdown formatting
                markdown_lines.append(
                    self._counter_to_markdown_line(counter_key, count, info, current_time)
                )

        # Add system stats if enabled
        if self.dump_system_stats:
            markdown_lines.append("")
            markdown_lines.append("## System Stats:")
            markdown_lines.append(self._system_stats_to_markdown_line())

        # Add pipeline and execution context info
        self._add_pipeline_info_to_markdown(markdown_lines, pipeline)

        markdown_content = "\n".join(markdown_lines)
        create_markdown_artifact(
            key=f"load-summary-{int(time.time())}",
            markdown=markdown_content,
            description="Load stage summary",
        )

    def on_start_trace(
        self, trace: PipelineTrace, step: PipelineStepTrace, pipeline: SupportsPipeline
    ) -> None:
        """Called when a pipeline trace starts - store the pipeline instance."""
        self.pipeline_instance = pipeline

    def _start(self, step: str) -> None:
        """Start tracking with Prefect task tags."""
        super()._start(step)
        self._update_task_tags()
        # Reset progress artifact IDs for new stage
        self.progress_artifact_ids = {}

    def _stop(self) -> None:
        """Stop tracking and create stage summary markdown artifact."""
        # Create artifact before calling parent's _stop() which clears counters
        if self.create_artifacts and self.counters is not None and len(self.counters) > 0 and self.pipeline_instance:
            try:
                self._create_stage_summary_markdown_artifact(self.pipeline_instance)
            except Exception as e:
                import logging
                logging.warning(f"PrefectCollector artifact creation error: {e}")

        # Update tags - if this was a load step, mark as completed
        if self.step.startswith("Load"):
            self._update_task_tags(completed=True)

        super()._stop()

    def on_end_trace_step(
        self,
        trace: PipelineTrace,
        step: PipelineStepTrace,
        pipeline: SupportsPipeline,
        step_info: Any,
        send_state: bool,
    ) -> None:
        """Called when a pipeline step ends - no action needed since artifacts are created in _stop()."""
        # Artifacts are now created in _stop() method, so no action needed here
        pass
        # check if there is a schema update

    def on_log(self) -> None:
        """Called when logging occurs - update progress artifacts."""
        if self.create_artifacts and self.counters:
            self._update_progress_artifacts()
        # also print the counters
        super().on_log()

    def _update_progress_artifacts(self) -> None:
        """Update or create progress artifacts for each stage."""
        # Don't update progress artifacts if counters are None (already cleared)
        if not self.counters:
            return
            
        try:
            # Update extract progress (Resources counter)
            self._update_stage_progress("Resources", "Extract")

            # Update normalize progress (Files counter)
            self._update_stage_progress("Files", "Normalize")

            # Update load progress (Jobs counter)
            self._update_stage_progress("Jobs", "Load")

        except Exception as e:
            import logging

            logging.warning(f"PrefectCollector progress artifact error: {e}")

    def _update_stage_progress(self, counter_name: str, stage_name: str) -> None:
        """Update progress artifact for a specific stage."""
        if counter_name not in self.counters:
            return

        progress = self._calculate_stage_progress(counter_name)
        description = self._get_stage_progress_description(counter_name, stage_name)

        if counter_name not in self.progress_artifact_ids:
            # Create new progress artifact
            self.progress_artifact_ids[counter_name] = create_progress_artifact(
                progress=progress, description=description
            )
        else:
            # Update existing progress artifact
            update_progress_artifact(
                artifact_id=self.progress_artifact_ids[counter_name], progress=progress
            )

    def _calculate_stage_progress(self, counter_name: str) -> float:
        """Calculate progress percentage for a specific counter."""
        count = self.counters.get(counter_name, 0)
        info = self.counter_info.get(counter_name)

        if info and info.total:
            # Calculate percentage based on counter with total
            return min(100.0, (count / info.total) * 100.0)
        else:
            # No total known, show 50% if processing
            return 50.0 if count > 0 else 0.0

    def _get_stage_progress_description(self, counter_name: str, stage_name: str) -> str:
        """Get description for a specific stage progress artifact."""
        # includ the total count in the description like this: `x of total Resources extracted`
        counter_info = self.counter_info.get(counter_name)
        total_count = counter_info.total if counter_info else "unknown"
        if counter_name == "Resources":
            return f"{stage_name} stage progress - x of {total_count} Resources extracted"
        elif counter_name == "Files":
            return f"{stage_name} stage progress - x of {total_count} Files processed"
        elif counter_name == "Jobs":
            return f"{stage_name} stage progress - x of {total_count} Jobs processed"
        else:
            return f"{stage_name} stage progress - {counter_name.lower()}"

    def _update_task_tags(self, completed: bool = False) -> None:
        """Update task tags based on current stage and completion status."""
        try:
            context = get_run_context()

            if completed:
                # Add completed tag
                if "completed" not in context.task_run.tags:
                    context.task_run.tags.append("completed")
            else:
                # Update stage tag
                stage_tag = self._get_stage_tag(self.step)

                # Remove previous stage tags
                self._remove_previous_stage_tags(context)

                # Add current stage tag
                if stage_tag and stage_tag not in context.task_run.tags:
                    context.task_run.tags.append(stage_tag)

        except Exception as e:
            import logging

            logging.warning(f"PrefectCollector tag update error: {e}")

    def _get_stage_tag(self, step: str) -> str:
        """Get the appropriate stage tag for the given step."""
        if step.startswith("Extract"):
            return "Extract"
        elif step.startswith("Normalize"):
            return "Normalize"
        elif step.startswith("Load"):
            return "Load"
        else:
            return "Unknown Stage"

    def _remove_previous_stage_tags(self, context: Union[FlowRunContext, TaskRunContext]) -> None:
        """Remove tags from previous pipeline stages."""
        stage_tags_to_remove = ["Extract", "Normalize", "Load"]

        # Remove stage tags
        for tag in stage_tags_to_remove:
            if tag in context.task_run.tags:
                context.task_run.tags.remove(tag)

    def _get_schema_changes(self, counters_summary: Dict[str, Any]) -> Dict[str, List[str]]:
        """Extract schema changes from counter summary."""
        # TODO: Parse SchemaUpdates counter messages
        # TODO: Parse applied_update messages
        return {"tables": [], "columns": []}
