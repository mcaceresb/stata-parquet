import pandas as pd
import os
df = pd.DataFrame([(1, 'a'), (2, 'b')], columns = ['n', 'l']) 
df.to_parquet('test-python.parquet')
