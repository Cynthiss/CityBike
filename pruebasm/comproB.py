import os
import pandas as pd

BRONZE_FOLDER = "data/bronze/"
SILVER_FOLDER = "data/silver/"

# Función para verificar los archivos procesados
def verify_bronze_file(file_path):
    try:
        df = pd.read_parquet(file_path)
        print(f"📂 Verificando archivo: {file_path}")
        
        # 1. Verificar que las columnas necesarias estén presentes
        required_columns = ['start_time', 'end_time', 'trip_duration', 'start_station_name', 'end_station_name', 'member_type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"❌ Faltan columnas en el archivo: {missing_columns}")
        else:
            print("✅ Todas las columnas necesarias están presentes.")
        
        # 2. Verificar tipos de datos
        if not pd.api.types.is_datetime64_any_dtype(df['start_time']):
            print("❌ 'start_time' no es de tipo datetime.")
        if not pd.api.types.is_datetime64_any_dtype(df['end_time']):
            print("❌ 'end_time' no es de tipo datetime.")
        if not pd.api.types.is_numeric_dtype(df['trip_duration']):
            print("❌ 'trip_duration' no es numérico.")
        
        # 3. Verificar valores nulos en columnas clave
        nulls = df[['start_station_name', 'end_station_name', 'member_type']].isnull().sum()
        if nulls.any():
            print(f"❌ Nulos encontrados en columnas clave: {nulls}")
        else:
            print("✅ No se encontraron nulos en columnas clave.")

        # 4. Verificar algunas filas aleatorias
        random_rows = df.sample(5)
        print(f"\nVerificación de algunas filas aleatorias:\n{random_rows[['start_time', 'end_time', 'trip_duration', 'start_station_name', 'end_station_name', 'member_type']]}")

        # 5. Verificar el rango de fechas en start_time y end_time
        start_time_range = df['start_time'].min(), df['start_time'].max()
        end_time_range = df['end_time'].min(), df['end_time'].max()
        print(f"Rango de 'start_time': {start_time_range}")
        print(f"Rango de 'end_time': {end_time_range}")
        
    except Exception as e:
        print(f"❌ Error al verificar el archivo {file_path}: {e}")

# Función para verificar todos los archivos en la carpeta Bronze
def verify_bronze_files():
    bronze_files = [f for f in os.listdir(BRONZE_FOLDER) if f.endswith(".parquet")]
    print(f"📄 Archivos en Bronze: {len(bronze_files)}")
    
    for file in bronze_files:
        file_path = os.path.join(BRONZE_FOLDER, file)
        verify_bronze_file(file_path)
    
    print("\n🏁 Verificación de Bronze completada.")

if __name__ == "__main__":
    verify_bronze_files()
