# G1 — Data access and locations

**Included**

| Concept | Write |
|---|---|
| getting to data (verb) | **access** |
| the adjective | **accessible**, **inaccessible** |
| the negative, as a verb | **cannot access** |
| where the data physically sits | **data location** |

**Excluded**

| Never | Because |
|---|---|
| reach, reaches, reachable, in reach of, out of reach, get to | one word for one concept — `access` |
| physical location, physical destination, physical dataset (in prose) | say **data location**; the method `data_location()` keeps its name |

**Rulings**

- **`access` is a verb for the act, and a noun for what a job may touch.** Keep the verb for the
  act of reading data: "the engine accesses the data", not "data access is one-way". The noun is
  legal only in the sense G9 defines — the `access` declaration, and the grant that answers it.
  dlt's docs already write "grant access", "read access" and "denied access" 40 times over; that
  usage was always compliant and stays.
- **`reach` is not always `access`.** "`SET SESSION` would not reach the cloned sessions" means
  *propagate to*, not *read data from*. A literal swap changes the meaning — restructure
  (Rule 9.1).
