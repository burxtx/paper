import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

i1="container_usage.csv"
o1="container_usage.parquet"
chunk_size=10**6
i=1

with pd.read_csv(i1, chunksize=chunk_size) as reader:
    for i, df in enumerate(reader):
        if i == 0:
            table = pa.Table.from_pandas(df)
            new_schema = table.schema
            writer = pq.ParquetWriter(o1, schema=new_schema)
        transformed_batch = pa.RecordBatch.from_pandas(df, schema=new_schema)
        writer.write_batch(transformed_batch)
    writer.close()
    
