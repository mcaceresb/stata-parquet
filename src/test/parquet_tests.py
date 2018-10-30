import pandas as pd
import numpy as np
df = pd.DataFrame(
    np.random.uniform(size = (10, 2)),
    columns = ["A", "B"]
)
df.to_parquet("test")
