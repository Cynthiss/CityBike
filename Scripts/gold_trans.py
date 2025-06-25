import os
import pandas as pd
from multiprocessing import Pool

# Configuración de carpetas
SILVER_FOLDER = "Data/silver/"
GOLD_FOLDER = "Data/gold/"
os.makedirs(GOLD_FOLDER, exist_ok=True)

# Cargar archivo Parquet
def load_parquet(file_path):
    print(f"📂 Cargando archivo: {file_path}")  # Muestra la ruta del archivo que se está cargando
    df = pd.read_parquet(file_path)
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')
    return df

# Función para procesar los datos y generar las agregaciones
def generate_aggregations(file_path):
    print(f"🔄 Procesando: {file_path}")
    
    # Cargar datos
    df = load_parquet(file_path)

    # 1. Datos diarios
    daily_data = df.groupby('start_time').agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()
    daily_data['date'] = daily_data['start_time'].dt.date
    daily_data.drop(columns=['start_time'], inplace=True)

    # 2. Datos mensuales
    df['year_month'] = df['start_time'].dt.to_period('M').astype(str)
    monthly_data = df.groupby('year_month').agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # 3. Datos por estación
    station_data = df.groupby(['year_month', 'start_station_name']).agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # 4. Datos por tipo de miembro
    member_type_data = df.groupby(['year_month', 'member_type']).agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # 5. Datos por hora
    df['start_hour'] = df['start_time'].dt.hour
    hourly_data = df.groupby(['year_month', 'start_hour']).agg(
        num_rides=('trip_duration', 'count')
    ).reset_index()

    # 6. Datos anuales
    df['year'] = df['start_time'].dt.year
    yearly_data = df.groupby('year').agg(
        num_rides=('trip_duration', 'count'),
        avg_trip_duration=('trip_duration', 'mean')
    ).reset_index()

    # Guardar los resultados en la carpeta Gold
    save_to_gold(daily_data, 'daily', file_path)
    save_to_gold(monthly_data, 'monthly', file_path)
    save_to_gold(station_data, 'station', file_path)
    save_to_gold(member_type_data, 'member_type', file_path)
    save_to_gold(hourly_data, 'hourly', file_path)
    save_to_gold(yearly_data, 'yearly', file_path)

# Función para guardar los datos agregados en el directorio Gold
def save_to_gold(df, data_type, file_path):
    output_file = f"{data_type}_{os.path.basename(file_path)}"
    output_path = os.path.join(GOLD_FOLDER, output_file)
    print(f"✅ Guardado en Gold: {output_path}")  # Muestra la ruta de salida
    df.to_parquet(output_path, index=False)

# Función para procesar todos los archivos en paralelo
def process_files_parallel():
    silver_files = [f for f in os.listdir(SILVER_FOLDER) if f.endswith(".parquet")]
    print(f"📄 Archivos en Silver: {len(silver_files)}")
    
    # Crear una lista completa de las rutas de los archivos
    file_paths = [os.path.join(SILVER_FOLDER, file) for file in silver_files]
    
    # Procesar los archivos en paralelo utilizando multiprocesamiento
    with Pool(processes=4) as pool:  # Ajusta el número de procesos según tu CPU
        pool.map(generate_aggregations, file_paths)

    print("\n🏁 Agregación Gold completada.")

# Ejecutar el procesamiento
if __name__ == "__main__":
    process_files_parallel()
