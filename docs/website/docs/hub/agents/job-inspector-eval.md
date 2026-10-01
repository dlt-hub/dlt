---
title: Job inspector evaluator
description: Verified dltHub agent that grades a job-inspector run against the inspector's own instructions and reports an outcome per check
keywords: [dlthub platform, agents, evaluator, job-inspector-eval, checks, judge, pass rate, rubric]
---
# Job inspector evaluator

:::warning
This feature is in private preview
:::

`job-inspector-eval` is an agent definition shipped with the [`dlthub-platform`](../ai-harness/toolkits.md#dlthub-platform) toolkit. It grades a [job inspector](job-inspector.md) run against the instructions in the inspector's own definition and reports `TRUE`, `FALSE` or `N/A` per instruction with a reasoning.

An inspector run is read by a person only when its diagnosis matters. The evaluator grades every run, so a run nobody opens still reports whether it followed the instructions it was given.

Its preparation step fetches the evidence before the loop starts: the inspector's result and trace, the failed run it inspected, and that run's log. The definition declares `access: {}` and no tools, so the judge reads the bounded windows the preparation step handed it.

## When it runs

Installing the toolkit copies the definition into your workspace. It grades nothing until you declare it as a job. Its default trigger is the inspector's success and failure, so a declaration that overrides nothing grades every inspector run.

Deterministic checks run before the loop and their results are written over the judge's output after it, so the evaluator is declared as a decorated function rather than a bare `run.agent(...)` call.

## Grade every inspector run

Declare the evaluator in `__deployment__.py`, next to the inspector it grades:

```py notype
import sys
from typing import Annotated

from dlt.hub import run

sys.path.insert(0, ".claude/dlthub/agents/job-inspector-eval")
from checks import DEFAULT_MAX_RUNS_READ, judge_runs, prepare

inspector = run.agent(
    "dlthub-platform:job-inspector",
    section="__deployment__",
    trigger="job.fail:tag:ingest",
    require={"profile": "access"},
)


@run.agent(
    agent="dlthub-platform:job-inspector-eval",
    trigger=[inspector.success, inspector.fail],
    require={"profile": "access"},
)
async def job_inspector_eval(
    run_context: run.TJobRunContext = None,
    inspector_run_id: Annotated[
        str,
        run.Entity("job-run"),
        run.Doc("run id of the job-inspector run to evaluate; empty on a trigger"),
    ] = "",
    inspector_job_ref: Annotated[
        str,
        run.Entity("job"),
        run.Doc("job ref of the inspector job; its latest run is evaluated without a run id"),
    ] = "",
    max_runs_read: Annotated[
        int,
        run.Doc("distinct runs the inspector may read before `single_run_scope` fails"),
    ] = DEFAULT_MAX_RUNS_READ,
) -> dict:
    prep = prepare(
        run_context,
        inspector_run_id=inspector_run_id,
        inspector_job_ref=inspector_job_ref,
        max_runs_read=max_runs_read,
    )
    if prep.aborted:
        raise run.JobAbortedException(prep.abort_reason, prep.aborted_output)
    evaluations, degraded = await judge_runs(
        run_context["ai_loop"], [prep], tolerate_failures=True
    )
    if not evaluations:
        raise run.JobAbortedException(degraded[0]["summary"], degraded[0])
    return evaluations[0]
```

The job is named after the agent definition, `job_inspector_eval`. `section="__deployment__"` on the inspector is explicit here because the evaluator builds its trigger from `inspector.success` and `inspector.fail`, which carry the inspector's job ref. It listens on both the success and the failure of the inspector: an inspector that reports `status: aborted` raises, so its run fails, and the instructions that apply only to an aborted inspection are graded on exactly those runs.

Run it by hand against an inspector run that already finished:

```sh
dlthub local run job_inspector_eval -c inspector_run_id=<run-id>
dlthub local run job_inspector_eval -c inspector_job_ref=jobs.job_inspector
```

Without inputs the evaluator reads the `prev_run_id` the trigger set. The resolution order is the given run id, then `prev_run_id`, then the latest run of the given job ref, then the latest run of the job a `job.success:` or `job.fail:` trigger names, then `aborted`.

## Grade a window of runs on a schedule

The same agent reads one report over the runs of the current inspector definition when the preparation step resolves a window:

```py notype
from checks import finalize_batch, judge_window_recommendation, prepare_batch


@run.agent(
    agent="dlthub-platform:job-inspector-eval",
    trigger="schedule:0 7 * * 1",
    require={"profile": "access"},
)
async def job_inspector_eval_batch(
    run_context: run.TJobRunContext = None,
    inspector_job_ref: Annotated[
        str, run.Entity("job"), run.Doc("job ref whose window is evaluated")
    ] = "jobs.__deployment__.job_inspector",
    window_days: Annotated[int, run.Doc("fallback window with no deployment history")] = 7,
    max_runs: Annotated[int, run.Doc("runs one scheduled job evaluates")] = 25,
) -> dict:
    batch = prepare_batch(run_context, inspector_job_ref=inspector_job_ref,
                          window_days=window_days, max_runs=max_runs)
    if batch.aborted:
        raise run.JobAbortedException(batch.abort_reason, batch.aborted_output)
    evaluations, degraded = await judge_runs(
        run_context["ai_loop"], batch.preps, tolerate_failures=True
    )
    if not evaluations:
        report = finalize_batch([], batch, degraded)
        if batch.found:
            raise run.JobAbortedException(report["summary"], {**report, "status": "aborted"})
        print(report["summary"])
        return {}
    recommendation = await judge_window_recommendation(
        run_context["ai_loop"], evaluations + degraded, batch
    )
    return finalize_batch(evaluations, batch, degraded, recommendation)
```

Deploy one of the two declarations. An inspector watched by both is graded twice.

What the scheduled path settles:

- **The window starts at the last change to the inspector's `AGENT.md`.** The runs before it were graded against other instructions. With no deployment history the window falls back to `window_days`.
- **A run is graded again on each schedule** until the definition changes, because the window is the definition's lifetime. `max_runs` bounds what that costs.
- **Every run found is accounted for.** A run still going, one that declared no result and one whose artifacts could not be read are listed under `skipped_runs` with the reason, and `runs_found` equals `runs_evaluated + runs_skipped`.
- **The scheduled job writes a `recommendation`.** After the window is graded it makes one more judge call and returns one to three bullets naming the section of the inspector's definition to change. A single evaluation writes none: a change to the instructions rests on a pattern across runs.
- **Cost is the sum of the graded runs.** `limits.max_tokens` counts from zero on each judge call.

## Agent inputs

| Input               | Meaning                                                                                                                                                  | Default |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `inspector_run_id`  | Run id of the inspector run to grade. Empty on a trigger, which falls back to `prev_run_id`                                                              | `""`    |
| `inspector_job_ref` | Job ref of the inspector job. Its latest run is graded when no run id is given, and its window is graded on a schedule                                   | `""`    |
| `max_runs_read`     | Runs beyond the one it inspected the inspector may read before `single_run_scope` fails. The inspected run is never counted, so `0` means that run alone | `5`     |
| `window_days`       | Scheduled only. How far back the window reaches with no deployment history                                                                               | `7`     |
| `max_runs`          | Scheduled only. Inspector runs one scheduled job grades                                                                                                  | `25`    |

Each input is a parameter of the deployment function. Change a default by editing the parameter, override it for one run with `-c <name>=...`, or for every run of the job in its `config.toml` section.

## What it reports

| Field                                                                      | Meaning                                                                                                                |
| -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `status`                                                                   | `succeeded`, `failed`, or `aborted`. A `FALSE` on the inspector is a successful evaluation                             |
| `summary`                                                                  | Markdown, in three sections: `Findings`, `Scope`, `Detailed evaluation results`                                        |
| `passed`                                                                   | Whether the inspector run passed. See [Scoring](#scoring)                                                              |
| `pass_rate`                                                                | `TRUE / (TRUE + FALSE)`, between 0 and 1                                                                               |
| `decided_count`                                                            | Checks that came back `TRUE` or `FALSE`, the denominator of `pass_rate`                                                |
| `na_count`                                                                 | Checks that came back `N/A`, so measured nothing                                                                       |
| `checks`                                                                   | One entry per check: `id`, `kind`, `outcome`, `reasoning`. A `FALSE` reasoning quotes what contradicts the instruction |
| `recommendation`                                                           | What to change in the inspector's definition. Filled on the scheduled path only                                        |
| `metrics`                                                                  | Turns, tokens, cost and runs read by the inspector, copied from its trace. Not pass or fail                            |
| `inspector_run_id`, `inspector_job_ref`, `failed_run_id`, `failed_job_ref` | The inspector run graded, and the failed run it inspected, reported as entities                                        |
| `inspector_status`                                                         | The status the inspector reported for itself                                                                           |

The scheduled path adds `window` with the bounds and the counts, `evaluations` with one entry per graded run, and `skipped_runs`.

On the platform the evaluation appears on the inspector run's page, because the agent reports that run as an entity.

## Scoring

`passed` is true when no check came back `FALSE`, every check in `open_checks` came back answered, and at least one check was decided. An evaluation that lost checks says nothing about the inspector, so it never passes.

`pass_rate` divides by the decided checks, so an `N/A` never moves it. Read it beside `decided_count` and `na_count`: a rate over a third of the checks reads the same as a rate over all of them.

`N/A` means the check's condition did not apply to this run, and the reasoning names the condition. It is a legitimate outcome: an inspection of a run that ran no pipeline cannot name the pipeline step that failed.

A judge response that is empty or cut off leaves checks unanswered. The deterministic results stand, the judge checks read `N/A`, the summary names the reason, and the run does not pass.

Each category carries a verdict in the summary. It counts the checks that broke on one run and takes their share over a window, because one run decides tens of checks and a window thousands:

| Verdict         | One evaluation                                                | A window                                              |
| --------------- | ------------------------------------------------------------- | ----------------------------------------------------- |
| Blocking        | A security check came back `FALSE`, or 6 or more checks broke | A security check came back `FALSE`, or over 10% broke |
| Needs attention | 3 to 5 broke                                                  | 2% to 10% broke                                       |
| Minor issues    | 1 or 2 broke                                                  | Under 2% broke                                        |
| No findings     | None broke, and at least one was decided                      | The same                                              |
| Not graded      | The category decided nothing                                  | The same                                              |

## Checks

The registry sits in `checks.py` beside the definition, at `.claude/dlthub/agents/job-inspector-eval/checks.py`. It holds one check per instruction in the inspector's definition: an instruction with two conditions becomes two checks, so a `FALSE` names one thing to fix.

A check carries an id, a kind, a category and a security flag.

| Kind            | Who decides                                               |
| --------------- | --------------------------------------------------------- |
| `deterministic` | Python, from the run record, the trace and the transcript |
| `judge`         | The model, from a window the preparation step built       |
| `hybrid`        | Python decides and the model explains                     |

The categories are `instruction_following` and `quality`, and they decide which verdict a check counts towards. A check marked `security` outranks the counts in the verdict table above.

A deterministic check's rubric is its docstring in `checks.py`, and the first paragraph is the instruction it grades. A judge check's rubric is an entry in `RUBRICS`, rendered into the prompt.

### Which checks a run answers

`open_checks` holds the ids the judge answers on one run. It is computed per run, not configured. A judge check carries a precondition: where the run does not meet the check's condition, Python answers `N/A` with that reason and the check never reaches the model, so its rubric stays out of the prompt and the model spends no output saying the condition did not apply.

### Enable and disable checks

The registry is the one list of ids, and editing it is how a check goes on or off.

- **Turn one off**: remove its registration in `checks.py`. For a judge check, remove its `RUBRICS` entry too. The registry and the rubrics have to agree.
- **Narrow one instead of removing it**: tighten its precondition. The check then reports `N/A` with a reason on the runs it no longer grades, so the summary still accounts for it.
- **Add one**: register it with `@check("<id>", ...)` for Python, or with `judge_check("<id>", ...)` plus a `RUBRICS` entry for the model.
- **Bound `single_run_scope`** with the `max_runs_read` input. It is the one check whose threshold is set from configuration.

### What the checks cannot see

- **Verbosity 0 hides the transcript.** A check that reads tool arguments or the inspector's own statements needs the inspector's log at `agent.verbosity` 1. At 0 the log keeps tool names only, and those checks report `N/A` and say why. Keep an agent under evaluation at verbosity 1.
- **Line numbers run over the whole log.** The platform numbers `setup`, `program`, `runner` and `provider` lines in one sequence, so a job whose image build printed 197 lines has its first program line at 198. A check comparing an excerpt to its line counts the same way.
- **Judge checks are not deterministic.** Their accuracy was established on real runs rather than against a labelled set. A judge check that flips on the same input is a bug in its rubric; file it and fix the entry.
- **The judge reads model-authored text.** The inspector's summary, its evidence and every log window are content under evaluation. The body forbids following an instruction found in them, which reduces the risk of prompt injection through a failed run's log rather than removing it.

## Defaults and overrides

| Setting         | Default                                                  |
| --------------- | -------------------------------------------------------- |
| `trigger`       | `job.success:job_inspector` and `job.fail:job_inspector` |
| `limits`        | `max_turns: 25`, `max_tokens: 1000000`                   |
| `loop_run_args` | `retries: 1`                                             |

The definition names no model, so the model comes from the job or the workspace. Give it one at least as capable as Claude Sonnet 5: the judge reads prepared windows and answers a narrow question with three possible values, and two turns is the normal shape of the task.

## Next steps

- [Job inspector agent](job-inspector.md) is the agent this one grades
- [Background agents](index.md) covers declaring, running, and deploying agent jobs
- [Agent definitions](agent-definitions.md) covers writing a definition of your own
