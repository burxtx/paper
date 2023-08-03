import dask.dataframe as dd
import pandas as pd

i1="machine_usage.csv"
o1="machine_usage_{}.csv"
chunk_size=10**6
i=1
# df = dd.read_csv(i1, blocksize="100MB", chunk_size=10**6)
with pd.read_csv(i1, chunksize=chunk_size, iterator=True) as reader:
    for chunk in reader:
        chunk.to_csv(o1.format(i))
        i+=1
        