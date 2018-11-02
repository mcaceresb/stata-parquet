import pandas as pd
import os
pd.read_parquet('test-stata.parquet')
pd.read_parquet('test-stata2.parquet')
df = pd.read_parquet('auto.parquet')
df
df.query('rep78 == -1').to_parquet('auto2.parquet')
pd.read_parquet('auto3.parquet')
# pd.DataFrame().to_parquet('auto4.parquet')
