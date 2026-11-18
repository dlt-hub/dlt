---
title: Stacked constraints (dlthub-client 0.4.0 + dlt 1.32.0)
since:
  dlthub-client: "0.4.0"
  dlt: "1.32.0"
---

Needs both releases: Preview until `dlthub-client` 0.4.0 **and** `dlt` 1.32.0 are out. Then "New in
dlthub-client 0.4.0, 1.32.0" for as long as one of them is within the last 3 minors.

With `DLT_DOCS_REQUIRES="dlthub-client@0.4.0=dlt>=1.34.0"`, `dlthub-client` 0.4.0 also implies `dlt>=1.34.0`: the
badge stays Preview until that `dlt` is released, and its tooltip says "requires dlt >= 1.34.0". A real
`dlthub-client` release declares this in its PyPI metadata (`Requires-Dist: dlt>=1.34.0`).

## Section needing a library only
<!-- since: dlthub-client 0.4.0 -->

One comment with a package name.

## Section stacking two libraries
<!-- since: dlthub-client 0.4.0 -->
<!-- since: dlt 1.32.0 -->

One comment per package, one after another.
