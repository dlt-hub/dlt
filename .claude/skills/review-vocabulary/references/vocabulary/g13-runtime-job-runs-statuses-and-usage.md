# G13 — Runtime job runs, statuses and usage

How the dltHub Runtime runs jobs and reports on them. Jobs, triggers, selectors and the runner
are G7; this group adds the runtime's statuses, shares and usage units. The rulings for every
runtime group are in G11.

**Included**

| Concept | Write |
|---|---|
| one execution of a job | **job run**; **run** alone when the context names only job runs (G11) |
| one dlt pipeline execution inside a job run | **pipeline run** |
| the state of a job run | **Pending, Starting, Running, Succeeded, Failed, Cancelling, Cancelled, Skipped** |
| a job run that another job's success starts | **follow-up run** |
| a label on a job, used to select jobs | **tag** |
| a shareable interactive job (marimo, Streamlit) | **data app** |
| the dlt dashboard deployed to a workspace | **workspace dashboard** |
| a public share of a job or data app | **public link** |
| the unit of compute consumption | **credit**; credits are **consumed** |
| wall-clock time a job run spends running | **run time** |
| the command that runs a job | `dlthub run` |

**Excluded**

| Never | Because |
|---|---|
| execute, execution, executed (for running a job) | **run**, **job run** |
| completed (as the status word in prose) | **succeeded**; the API value `completed` stays in code and contracts |
| notebook, app, notebook/app (for a data app) | **data app**; the `expose.category` value `notebook` stays |
| public URL, public trigger URL | **public link**; the field `public_url` stays |
| compute minutes, runtime minutes, credit hours | **credits**; the error code `compute_minutes_exhausted` stays |
| launch (for running a job) | **run**: the command is `dlthub run` |
| trigger (as a verb) | **run** or **start**; the command name `dlthub job trigger` stays |
| followup (in prose) | **follow-up run**; `followup` identifiers keep their names |

**Rulings**

- **`launcher` is legal, `launch` is not.** The launcher is dlt code (G7). The verb "launch" is
  banned for running a job; it is the old name of `dlthub run`.
- **`completed` is a contract.** `RunStatus.COMPLETED` and the wire value `completed` stay. Prose
  and user-facing text say **succeeded**.
- **The dashboard is the workspace dashboard.** The web app's landing page is the **Overview**,
  never "the dashboard".
- **A pipeline run is never a job run.** One job run produces zero or more pipeline runs. A tool
  that lists pipeline runs says "pipeline run" in every sentence that could be read either way.
- **Newly non-compliant:** "execute(d)" for runs, "completed" in status prose, "notebook/app",
  "launch". **Newly compliant:** bare "run" in single-kind contexts.
