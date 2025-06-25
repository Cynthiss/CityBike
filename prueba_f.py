
import pandas as pd

# Leer el archivo Parquet
df = pd.read_parquet("Data/silver/2013-06-01_2013-06-30.parquet")

# Verificar los primeros registros
print(df.head())
