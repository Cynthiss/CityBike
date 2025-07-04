import os
import pandas as pd

# Configuración de carpetas
GOLD_FOLDER = "Data/gold/"
CONSOLIDATED_FOLDER = "Data/goldc/"
os.makedirs(CONSOLIDATED_FOLDER, exist_ok=True)

# Definir los tipos que solo se concatenan (no agrupar)
CONCATENATE_ONLY = ['station', 'member_type']

# Función para consolidar archivos
def consolidate_files(data_type):
    files = [f for f in os.listdir(GOLD_FOLDER) if f.startswith(data_type) and f.endswith(".parquet")]

    print(f"📄 Archivos para consolidar tipo {data_type}: {len(files)}")

    if not files:
        print(f"⚠️ No se encontraron archivos para '{data_type}'.")
        return

    # Leer y concatenar
    all_data = []
    for file in files:
        file_path = os.path.join(GOLD_FOLDER, file)
        print(f"📂 Cargando archivo: {file_path}")
        df = pd.read_parquet(file_path)
        all_data.append(df)

    consolidated_data = pd.concat(all_data, ignore_index=True)

    # Si es station o member_type solo concatenar
    if data_type in CONCATENATE_ONLY:
        final_data = consolidated_data
        print(f"🔗 Consolidación simple (sin agrupación) para {data_type}.")

    else:
        # Definir claves según tipo
        if data_type == 'daily':
            keys = ['date']
        elif data_type == 'monthly':
            keys = ['year_month', 'start_hour']
        elif data_type == 'hourly':
            keys = ['year_month', 'hour']
        elif data_type == 'yearly':
            keys = ['year']
        else:
            print(f"⚠️ Tipo '{data_type}' no reconocido para agrupación. Solo se concatenó.")
            final_data = consolidated_data
            keys = None

        if keys:
            print(f"🔗 Agrupando por {keys}")
            final_data = consolidated_data.groupby(keys, dropna=False).sum(numeric_only=True).reset_index()

    # Guardar
    output_file = f"{data_type}_consolidated.parquet"
    output_path = os.path.join(CONSOLIDATED_FOLDER, output_file)
    print(f"✅ Guardado consolidado en: {output_path}")
    final_data.to_parquet(output_path, index=False)

def consolidate_all():
    data_types = ['daily', 'monthly', 'station', 'member_type', 'hourly', 'yearly']
    for data_type in data_types:
        consolidate_files(data_type)

    print("\n🏁 Consolidación completada.")

if __name__ == "__main__":
    consolidate_all()
