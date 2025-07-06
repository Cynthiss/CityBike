import os
import pandas as pd
from multiprocessing import Pool

# Carpetas
SILVER_FOLDER = "Data/silver/"
GOLD_FOLDER = "Data/gold/"
os.makedirs(GOLD_FOLDER, exist_ok=True)

# Función para cargar cada archivo Parquet
def load_parquet(file_path):
    print(f"📂 Cargando archivo: {file_path}")
    df = pd.read_parquet(file_path)

    # Asegurarse de que columnas temporales existan
    if 'date' not in df.columns:
        df['date'] = pd.to_datetime(df['start_time'], errors='coerce').dt.date
    if 'year_month' not in df.columns:
        df['year_month'] = pd.to_datetime(df['start_time'], errors='coerce').dt.to_period('M').astype(str)
    if 'start_hour' not in df.columns:
        df['start_hour'] = pd.to_datetime(df['start_time'], errors='coerce').dt.hour
    if 'year' not in df.columns:
        df['year'] = pd.to_datetime(df['start_time'], errors='coerce').dt.year

    return df

# Agregaciones
def generate_aggregations(file_path):
    print(f"🔄 Procesando: {file_path}")
    df = load_parquet(file_path)

    # 1. Daily
    daily_data = df.groupby('date').agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # 2. Monthly
    monthly_data = df.groupby('year_month').agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # 3. Station
    station_data = df.groupby(['year_month', 'start_station_name']).agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # 4. Member Type
    member_type_data = df.groupby(['year_month', 'member_type']).agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # 5. Hourly
    hourly_data = df.groupby(['year_month', 'start_hour']).agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # 6. Yearly
    yearly_data = df.groupby('year').agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # 7. Coordenadas únicas
    stations_start = df[['start_station_name', 'start_lat', 'start_lng']].dropna().drop_duplicates()
    stations_start.columns = ['station_name', 'lat', 'lng']

    stations_end = df[['end_station_name', 'end_lat', 'end_lng']].dropna().drop_duplicates()
    stations_end.columns = ['station_name', 'lat', 'lng']

    station_coords = pd.concat([stations_start, stations_end], ignore_index=True).drop_duplicates(subset='station_name')

    # 8. Rides por birth_year
    rides_birth_year = pd.DataFrame()
    if 'birth_year' in df.columns:
        df['birth_year'] = pd.to_numeric(df['birth_year'], errors='coerce')
        rides_birth_year = df.dropna(subset=['birth_year', 'year_month']) \
            .groupby(['birth_year', 'year_month']) \
            .size().reset_index(name='num_rides')

    # Guardar resultados
    save_to_gold(daily_data, 'daily', file_path)
    save_to_gold(monthly_data, 'monthly', file_path)
    save_to_gold(station_data, 'station', file_path)
    save_to_gold(member_type_data, 'member_type', file_path)
    save_to_gold(hourly_data, 'hourly', file_path)
    save_to_gold(yearly_data, 'yearly', file_path)
    save_to_gold(station_coords, 'station_coordinates', file_path)
    
    if not rides_birth_year.empty:
        save_to_gold(rides_birth_year, 'rides_by_birth_year', file_path)

# Guardar cada agregación en carpeta Gold
def save_to_gold(df, data_type, file_path):
    output_file = f"{data_type}_{os.path.basename(file_path)}"
    output_path = os.path.join(GOLD_FOLDER, output_file)
    print(f"✅ Guardado en Gold: {output_path}")
    df.to_parquet(output_path, index=False)

# Ejecutar todos los archivos en paralelo
def process_files_parallel():
    silver_files = [f for f in os.listdir(SILVER_FOLDER) if f.endswith(".parquet")]
    print(f"📄 Archivos en Silver: {len(silver_files)}")

    file_paths = [os.path.join(SILVER_FOLDER, file) for file in silver_files]

    with Pool(processes=4) as pool:  # Ajusta si necesitas más/menos procesos
        pool.map(generate_aggregations, file_paths)

    print("\n🏁 Agregación Gold completada.")

# Ejecutar
if __name__ == "__main__":
    process_files_parallel()
