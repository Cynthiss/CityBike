# import glob
# import os

# bronze_files = glob.glob("data/bronze/*.parquet")
# for f in bronze_files:
#     os.remove(f)
# print("🧹 Archivos Parquet eliminados correctamente.")


#---------------------------------
# import os

# RAW_FOLDER = "data/raw/"
# BRONZE_FOLDER = "data/bronze/"

# # 1. Archivos CSV originales
# csv_files = [f for f in os.listdir(RAW_FOLDER) if f.endswith(".csv")]
# print(f"📦 CSV originales: {len(csv_files)}")

# # 2. Archivos Parquet generados
# parquet_files = [f for f in os.listdir(BRONZE_FOLDER) if f.endswith(".parquet")]
# print(f"📁 Parquet en Bronze: {len(parquet_files)}")

# # 3. Archivos CSV sin ningún Parquet asociado
# processed_prefixes = set(f.replace("_part0.parquet", "").replace(".parquet", "").split("_part")[0] for f in parquet_files)
# unprocessed = [f for f in csv_files if f.replace(".csv", "") not in processed_prefixes]

# if unprocessed:
#     print("\n⚠️ Archivos CSV sin procesamiento:")
#     for file in unprocessed:
#         print(" -", file)
# else:
#     print("\n✅ Todos los CSV tienen al menos un Parquet generado.")
#------

# import os

# BRONZE_FOLDER = "data/bronze/"
# SILVER_FOLDER = "data/silver/"

# bronze_files = sorted([f for f in os.listdir(BRONZE_FOLDER) if f.endswith(".parquet")])
# silver_files = sorted([f for f in os.listdir(SILVER_FOLDER) if f.endswith(".parquet")])

# print(f"📦 Archivos en Bronze: {len(bronze_files)}")
# print(f"📁 Archivos en Silver: {len(silver_files)}")

# # Buscar archivos que faltan en Silver
# missing = set(bronze_files) - set(silver_files)

# if missing:
#     print("\n⚠️ Faltan archivos en Silver:")
#     for f in missing:
#         print(" -", f)
# else:
#     print("\n✅ Todos los archivos fueron procesados correctamente a Silver.")

#-------------------------------

import os
import pandas as pd

GOLD_FOLDER = "data/gold/"

files = [f for f in os.listdir(GOLD_FOLDER) if f.endswith('.parquet')]

for file in files:
    path = os.path.join(GOLD_FOLDER, file)
    df = pd.read_parquet(path)
    print(f"\n📄 {file}")
    #print(f"🔢 Registros: {len(df)}")
    print(df.dtypes)
    #print(df.head(3))  # Muestra las primeras filas para ver columnas y formato
