import os
import pandas as pd

GOLD_FOLDER = "data/gold/"
CSV_FOLDER = "data/gold_csv/"
os.makedirs(CSV_FOLDER, exist_ok=True)

parquet_files = [f for f in os.listdir(GOLD_FOLDER) if f.endswith('.parquet')]

for file in parquet_files:
    parquet_path = os.path.join(GOLD_FOLDER, file)
    csv_path = os.path.join(CSV_FOLDER, file.replace(".parquet", ".csv"))
    
    try:
        df = pd.read_parquet(parquet_path)

        # Convertir todas las columnas numéricas a enteros si no hay decimales
        for col in df.columns:
            if pd.api.types.is_float_dtype(df[col]):
                if df[col].dropna().apply(float.is_integer).all():
                    df[col] = df[col].astype("Int64")  # Maneja también nulos
                else:
                    df[col] = df[col].round(2)  # Mantiene solo dos decimales

        df.to_csv(csv_path, index=False)
        print(f"✅ Exportado sin errores: {file} → {csv_path}")

    except Exception as e:
        print(f"❌ Error exportando {file}: {e}")
