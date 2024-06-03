import duckdb

conn = duckdb.connect()
conn.sql('from duckdb_extensions()').show()  # checking if extensions are loaded
conn.install_extension("./postgres_scanner.duckdb_extension")  # this is where I copied it manually
conn.sql("LOAD postgres;")
conn.sql('from duckdb_extensions()').show()  # checking if extensions are loaded
