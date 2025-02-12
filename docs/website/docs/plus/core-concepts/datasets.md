# Datasets  
  
A dataset is a physical collection of data and dlt metadata including the schema on a destination. One destination can have multiple datasets; for now, datasets are bound to a physical destination, but this may change in future iterations.

dlt packages require (by default) that you declare the datasets you intend to use in the package and specify the destinations where they may be materialized. Datasets in the package create a data catalog that can be used to discover schemas, read, and write data.

Through datasets, dlt+ fully leverages schema inference of dlt. Cataloging is automated.

Datasets are a fundamental unit of governance in the package:

1. You can enable and disable them per profile.
2. You can set schema contracts also per profile.
3. [🚧 WIP!] You can set data contracts (i.e., read-only tables).
4. [🚧 WIP!] You can apply a different set of contracts per particular user.
