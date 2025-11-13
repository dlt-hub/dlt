# ruff: noqa: E501

import textwrap


def dlt_welcome() -> str:
    """Learn about dlt Assistant features."""
    return textwrap.dedent("""\
        Inform the user of the following:
        - You're the "dlt Assistant";
        - You can teach about dlt, help write pipelines, and explore data;
        - You will ask to use tools to access pipelines & data to accurate and up-to-date information;
        - The user can help you by referencing specific documentation or code in their question
        """)


def dlt_select_loading_strategy() -> str:
    """Step-by-step process to select the right loading strategy."""
    return textwrap.dedent(
        """Ask the user a series of questions to select the right loading strategy then provide boilerplate code.

        ## Write dispositions
        REPLACE: All data is removed from destination and is replaced by the data from the source.
        APPEND: The data from the source is appended to the destination.
        MERGE: Add new data to the destination and resolve items already present based on `merge_key` or `primary_key`.

        ## Write dispositions questions
        Ask the questions sequentially in a multi-turn conversation to find the appropriate write disposition.

        1. Is the data stateful?
            If NO: use `APPEND`
            ```python
            @dlt.resource(write_disposition="append")
            ```

            If YES: continue

        2. Do you need to track the history of data changes?
            If YES: use `MERGE - SCD2`
            ```python
            # can use a `merge_key` for incremental loading
            @dlt.resource(write_disposition={"disposition": "merge", "strategy": "scd2"})
            ```
            If NO: continue

        3. Can you request the data incrementally / you don't have to load the full dataset?
            If YES: use `MERGE - DELETE-INSERT`
            ```python
            # use a `primary_key` or `merge_key` to deduplicate data
            @dlt.resource(primary_key="id", write_disposition={"disposition": "merge", "strategy": "delete-insert"})
            ```
            If NO: use `REPLACE`
            ```python
            @dlt.resource(write_disposition="replace")
            ```
        """
    )


__prompts__ = (
    dlt_welcome,
    dlt_select_loading_strategy,
)
