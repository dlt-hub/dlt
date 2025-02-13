# Datasets  
  
A dataset is a physical collection of data and dlt metadata including the schema on a destination. One destination can have multiple datasets; for now, datasets are bound to a physical destination, but this may change in future iterations.

By treating datasets as individual entites, dlt+ enables data cataloging and data governance.  
  
#### Data cataloging  
  
Datasets automatically create data catalogs that can be used to discover schema, and read and write data.

#### Data governance  
  
Datasets are a fundamental unit of governance in the package. Using the declarative interface of dlt+, it is possible to control:  
1. where they are materialized: you can specify which destinations you would like to materialize the datasets in.
2. who can access them: you can enable and disable them per profile.
3. who can modify the schemas: you can also set schema contracts per profile.
