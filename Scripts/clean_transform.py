import os
import pandas as pd

RAW_FOLDER = "data/raw/"
BRONZE_FOLDER = "data/bronze/"
CHUNKSIZE = 500_000

# Definir mapeo flexible de columnas a nombres estándar
COLUMN_MAPPING = {
    'start_time': ['starttime', 'start_time', 'started_at', 'start time'],
    'end_time': ['stoptime', 'end_time', 'ended_at', 'stop time'],
    'trip_duration': ['tripduration', 'trip_duration', 'trip duration'],
    'start_station_name': ['start_station_name', 'start station name'],
    'end_station_name': ['end_station_name', 'end station name'],
    'member_type': ['member_casual', 'usertype', 'user_type'],
}

STANDARD_COLUMNS = list(COLUMN_MAPPING.keys())

def normalize_columns(df):
    col_map = {}
    for standard_col, possible_names in COLUMN_MAPPING.items():
        for col in df.columns:
            if col.lower().strip().replace(" ", "_") in [name.lower().strip().replace(" ", "_") for name in possible_names]:
                col_map[col] = standard_col
                break
    df.rename(columns=col_map, inplace=True)

    # Asegurar todas las columnas estándar existen
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    return df[STANDARD_COLUMNS]  # Reordenar

def clean_and_convert_all():
    os.makedirs(BRONZE_FOLDER, exist_ok=True)
    csv_files = [f for f in os.listdir(RAW_FOLDER) if f.endswith(".csv")]
    print(f"📄 Archivos CSV encontrados: {len(csv_files)}")

    for file in csv_files:
        file_path = os.path.join(RAW_FOLDER, file)
        base_name = file.replace(".csv", "")

        try:
            print(f"\n🧼 Procesando: {file}")

            for i, chunk in enumerate(pd.read_csv(file_path, chunksize=CHUNKSIZE, low_memory=False)):
                # Normalizar nombres de columnas
                chunk.columns = chunk.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")

                # Asignar nombres estándar y crear columnas faltantes
                chunk = normalize_columns(chunk)

                # Limpieza básica
                chunk.dropna(subset=['start_time', 'end_time'], inplace=True)
                chunk['start_time'] = pd.to_datetime(chunk['start_time'], errors='coerce')
                chunk['end_time'] = pd.to_datetime(chunk['end_time'], errors='coerce')
                chunk.dropna(subset=['start_time', 'end_time'], inplace=True)

                # Guardar como Parquet
                out_file = f"{base_name}_part{i}.parquet"
                out_path = os.path.join(BRONZE_FOLDER, out_file)
                chunk.to_parquet(out_path, index=False)
                print(f"✅ Guardado: {out_file}")

        except Exception as e:
            print(f"❌ Error procesando {file}: {e}")

    print("\n🎉 Limpieza Bronze completada.")

if __name__ == "__main__":
    clean_and_convert_all()

