import os
import pandas as pd 
from multiprocessing import Pool

RAW_FOLDER = "data/raw/"
BRONZE_FOLDER = "data/bronze/"
CHUNKSIZE = 500_000
YEAR = 2019
  # Procesar solo los archivos de 2013

# Crear la carpeta de Bronze si no existe
os.makedirs(BRONZE_FOLDER, exist_ok=True)

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

def process_file(file, start_date, end_date):
    base_name = os.path.basename(file).replace(".csv", "")  # Obtener solo el nombre base del archivo
    all_month_data = []

    try:
        print(f"\n🧼 Procesando: {file}")

        # Leer el archivo completo
        df = pd.read_csv(file, low_memory=False)

        # Normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")

        # Asignar nombres estándar y crear columnas faltantes
        df = normalize_columns(df)

        # Filtrar por el rango de fechas mensual
        df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        filtered_df = df[(df['start_time'] >= start_date) & (df['start_time'] <= end_date)]

        if not filtered_df.empty:
            # Limpieza básica
            filtered_df.dropna(subset=['start_time', 'end_time'], inplace=True)
            filtered_df['start_time'] = pd.to_datetime(filtered_df['start_time'], errors='coerce')
            filtered_df['end_time'] = pd.to_datetime(filtered_df['end_time'], errors='coerce')
            filtered_df.dropna(subset=['start_time', 'end_time'], inplace=True)

            # Agregar los datos del mes a la lista
            all_month_data.append(filtered_df)

    except Exception as e:
        print(f"❌ Error procesando {file}: {e}")

    # Concatenar todos los archivos del mes en un solo DataFrame
    if all_month_data:
        month_data_df = pd.concat(all_month_data, ignore_index=True)

        # Guardar como Parquet
        out_file = f"{start_date}_{end_date}.parquet"
        out_path = os.path.join(BRONZE_FOLDER, out_file)
        month_data_df.to_parquet(out_path, index=False)
        print(f"✅ Guardado: {out_file}")

def clean_and_convert_all():
    # Buscar archivos CSV en la carpeta y subcarpetas, solo del año 2013
    csv_files = []
    for root, dirs, files in os.walk(RAW_FOLDER):
        for file in files:
            if file.endswith(".csv") and str(YEAR) in file:
                csv_files.append(os.path.join(root, file))
    
    print(f"📄 Archivos CSV encontrados para {YEAR}: {len(csv_files)}")

    # Definir las fechas mensuales de 2013
    date_range = pd.date_range(start=f'{YEAR}-01-01', end=f'{YEAR}-12-31', freq='MS')  # 'MS' = inicio de mes

    # Crear un pool de procesos para paralelizar
    with Pool() as pool:
        for single_date in date_range:
            start_date = single_date.strftime('%Y-%m-%d')
            end_date = (single_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime('%Y-%m-%d')
            
            print(f"\n🔄 Procesando datos para el mes: {start_date} a {end_date}")

            # Verificar si el archivo ya fue procesado (se guarda en Parquet por mes)
            out_file = f"{start_date}_{end_date}.parquet"
            out_path = os.path.join(BRONZE_FOLDER, out_file)
            
            if os.path.exists(out_path):
                print(f"⏩ El archivo {out_file} ya existe en Bronze. Saltando procesamiento.")
                continue  # Si el archivo ya existe, no procesar nuevamente

            # Procesar los archivos en paralelo por mes
            pool.starmap(process_file, [(file, start_date, end_date) for file in csv_files])

    print("\n🎉 Limpieza Bronze completada.")

if __name__ == "__main__":
    clean_and_convert_all()
