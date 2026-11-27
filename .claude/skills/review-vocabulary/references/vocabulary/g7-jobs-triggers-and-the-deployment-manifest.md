# G7 — Jobs, triggers and the deployment manifest

**Included**

| Concept | Write |
|---|---|
| the unit a workspace deploys and runs | **job** |
| one execution of it | **job run** |
| its identity | **job ref** |
| its entry in the manifest | **job definition** |
| the dlt module that starts a job | **launcher** |
| the machine the platform gives the job | **runner** |
| the dltHub platform itself | **runtime** |
| the string that starts a job | **trigger** |
| the pattern that expands into triggers | **selector** |
| the trigger a manual run stands in for | **default trigger** |
| the file that describes every job | **deployment manifest** |

**Excluded**

| Never | Because |
|---|---|
| task, workload, script (for a dlt job) | one name — **job**; the runtime's `Script` model keeps its own name |
| job execution, invocation (for one run) | **job run**, the noun the manifest and the beacon use |
| primary trigger | **default trigger**, the name of the manifest field |
| manifest (unqualified, for `AGENT.md`) | **deployment manifest** is the only manifest; see G8 |

**Rulings**

- **`run` is a verb; `job run` is the noun.** "dlt runs the job" and "the job run failed". The
  command `dlthub local run` is a technical name (Rule 8.6) and stays.
- **launcher, runner and runtime are three things.** The launcher is dlt code. The runner is a
  machine. The runtime is the platform. Never swap them, and never write "the runtime launches".
- **`task` is legal in two places only:** the work an agent must do (G8), and an Airflow task when
  the sentence says "Airflow task". It is never a dlt job.
