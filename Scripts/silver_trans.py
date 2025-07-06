import os
import pandas as pd
#from azure.storage.blob import BlobServiceClient

BRONZE_FOLDER = "data/bronze/"
SILVER_FOLDER = "data/silver/"
CHUNKSIZE = 500_000

# Crear la carpeta de Silver si no existe
os.makedirs(SILVER_FOLDER, exist_ok=True)

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

OPTIONAL_COLUMNS = ['start_lat', 'start_lng', 'end_lat', 'end_lng', 'birth_year']


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

def transform_to_silver():
    bronze_files = [f for f in os.listdir(BRONZE_FOLDER) if f.endswith(".parquet")]
    print(f"📄 Archivos en Bronze: {len(bronze_files)}")

    for file in bronze_files:
        try:
            print(f"\n🔄 Transformando: {file}")
            df = pd.read_parquet(os.path.join(BRONZE_FOLDER, file))

            # Convertir en datetime
            for col in ["start_time", "end_time"]:
                if col in df.columns: 
                    df[col] = pd.to_datetime(df[col], errors="coerce")
            
            # Calcular trip_duration si no existe
            if 'trip_duration' not in df.columns or df['trip_duration'].isnull().all():
                df['trip_duration'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60

            # Quitar valores absurdos o negativos
            df = df[df['trip_duration'] > 0]

            # Extraer campos de tiempo
            df['start_hour'] = df['start_time'].dt.hour
            df['start_day'] = df['start_time'].dt.day
            df['start_month'] = df['start_time'].dt.month
            df['year_month'] = df['start_time'].dt.to_period("M").astype(str)
            df['date'] = df['start_time'].dt.date

            # Estandarizar tipos de miembro
            if 'member_type' in df.columns:
                df['member_type'] = df['member_type'].str.lower().replace({
                    'subscriber': 'member',
                    'customer': 'casual'
                })
            
            for col in OPTIONAL_COLUMNS:
                if col not in df.columns:
                    df[col] = pd.NA

            # Verificar si el archivo ya existe en Silver
            out_path = os.path.join(SILVER_FOLDER, file)
            if os.path.exists(out_path):
                print(f"⏩ El archivo {file} ya existe en Silver. Saltando procesamiento.")
                continue  # Si el archivo ya existe, saltar este archivo

            # Guardar archivo transformado
            df.to_parquet(out_path, index=False)
            print(f"✅ Guardado en Silver: {file}")

        except Exception as e:
            print(f"❌ Error con {file}: {e}")

    print("\n🏁 Transformación Silver completada.")

if __name__ == "__main__":
    transform_to_silver()
