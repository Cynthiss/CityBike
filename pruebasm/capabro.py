import os
import pandas as pd

BRONZE_FOLDER = "data/bronze/"
SILVER_FOLDER = "data/silver/"
os.makedirs(SILVER_FOLDER, exist_ok=True)

# Función para procesar y limpiar el archivo
def process_bronze_file(file_path):
    try:
        df = pd.read_parquet(file_path)

        # Verificación de columnas necesarias
        required_columns = ['start_time', 'end_time', 'trip_duration', 'start_station_name', 'end_station_name', 'member_type']
        for col in required_columns:
            if col not in df.columns:
                print(f"❌ Columna '{col}' no encontrada en el archivo: {file_path}")
                return  # Si alguna columna falta, no procesamos el archivo
        
        # Limpiar columnas numéricas (como trip_duration)
        df['trip_duration'] = pd.to_numeric(df['trip_duration'], errors='coerce')  # Convertir valores no numéricos a NaN
        
        # Reemplazar valores nulos en columnas clave
        df['start_station_name'].fillna('Desconocido', inplace=True)
        df['end_station_name'].fillna('Desconocido', inplace=True)
        df['member_type'].fillna('Desconocido', inplace=True)

        # Validar que start_time y end_time sean fechas válidas
        df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')

        # Eliminar filas con fechas nulas (esto puede variar según el contexto)
        df.dropna(subset=['start_time', 'end_time'], inplace=True)

        # Guardar el archivo procesado
        output_path = os.path.join(SILVER_FOLDER, os.path.basename(file_path))
        df.to_parquet(output_path, index=False)
        print(f"✅ Archivo procesado y guardado: {output_path}")
        
    except Exception as e:
        print(f"❌ Error al procesar el archivo {file_path}: {e}")

# Procesar todos los archivos en la carpeta de Bronze
def process_bronze_files():
    bronze_files = [f for f in os.listdir(BRONZE_FOLDER) if f.endswith(".parquet")]
    for file in bronze_files:
        file_path = os.path.join(BRONZE_FOLDER, file)
        process_bronze_file(file_path)
    print("\n🏁 Proceso de Bronze completado.")

if __name__ == "__main__":
    process_bronze_files()
