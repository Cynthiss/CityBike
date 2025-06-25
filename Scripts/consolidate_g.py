import os
import pandas as pd

# Configuración de carpetas
GOLD_FOLDER = "Data/gold/"
CONSOLIDATED_FOLDER = "Data/consolidated/"
os.makedirs(CONSOLIDATED_FOLDER, exist_ok=True)

# Función para consolidar los archivos por tipo
def consolidate_files(data_type):
    # Obtener todos los archivos generados por el tipo de agregación (diario, mensual, etc.)
    files = [f for f in os.listdir(GOLD_FOLDER) if f.startswith(data_type) and f.endswith(".parquet")]
    
    print(f"📄 Archivos para consolidar tipo {data_type}: {len(files)}")
    
    # Leer y concatenar todos los archivos
    all_data = []
    for file in files:
        file_path = os.path.join(GOLD_FOLDER, file)
        print(f"📂 Cargando archivo: {file_path}")
        df = pd.read_parquet(file_path)
        all_data.append(df)
    
    # Concatenar todos los DataFrames en uno solo
    consolidated_data = pd.concat(all_data, ignore_index=True)
    
    # Guardar el DataFrame consolidado en un archivo Parquet
    output_file = f"{data_type}_consolidated.parquet"
    output_path = os.path.join(CONSOLIDATED_FOLDER, output_file)
    print(f"✅ Guardado consolidado en: {output_path}")
    consolidated_data.to_parquet(output_path, index=False)

# Función para consolidar todos los tipos
def consolidate_all():
    # Tipos de agregación que quieres consolidar
    data_types = ['daily', 'monthly', 'station', 'member_type', 'hourly', 'yearly']
    
    # Consolida todos los tipos
    for data_type in data_types:
        consolidate_files(data_type)
    
    print("\n🏁 Consolidación completada.")

# Ejecutar el procesamiento de consolidación
if __name__ == "__main__":
    consolidate_all()
