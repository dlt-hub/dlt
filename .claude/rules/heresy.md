Heresy: issues in code that not only must be **avoided** but actively **detected and removed**.

## llm heresy
* llm talks to itself in docstrings instead of writing code comments in relevant places
* llm talks about some obscure downstream cases in upstream code (ie. base class is full of details of particular implementation or explains particular use case)
* llm invents obscure terminology and uses it everywhere. does not follow terminology used in docs
* llm writes extremely detailed and complex sentences to explain simple things

## arithmetic heresy
* decimals are converted to binary floats and back
* binary floats are used to construct decimals. ie. `Decimal(1.1)`

## datetime heresy
* naive datetimes are used without a good reason. the only exception are end user requirements
* pendulum is used in new code. use the stdlib counterparts: `datetime` with an explicit
  `timezone`/`ZoneInfo`, and the `ensure_datetime*` helpers instead of `ensure_pendulum_datetime*`.
  NOTE: existing pendulum usage MAY be tolerated where it is hard to change

## terms heresy
* use of following words constitutes heresy: gate/gating, graft

## comments heresy
* block separator comments as below
```
#------------------------------
# comment
#------------------------------
```
or similar. actually any form