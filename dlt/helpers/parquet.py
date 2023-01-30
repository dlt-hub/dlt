# https://blog.datasyndrome.com/python-and-parquet-performance-e71da65269ce
# parquet is partitioned ^^^
# https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.select
# https://stackoverflow.com/questions/71428738/is-it-possible-to-append-rows-to-an-existing-arrow-pyarrow-table
# adding data to pyarrow table is concatenating many tables
# https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html - write table
# https://stackoverflow.com/questions/47113813/using-pyarrow-how-do-you-append-to-parquet-file - keep the writer open
# row labels - unique ids of the rows. seems pyarrow adds them automatically and we cannot replace with dlt one

# https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
# with datasets -> you can easily add columns, partition and data updates - all as new files.

# you need to split the files yourself https://stackoverflow.com/questions/68678394/pyarrow-parquetwriter-is-there-a-way-to-limit-the-size-of-the-output-file-spli