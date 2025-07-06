import os
import pandas as pd

# Carpetas
GOLD_FOLDER = "Data/gold/"
CONSOLIDATED_FOLDER = "Data/goldc/"
os.makedirs(CONSOLIDATED_FOLDER, exist_ok=True)

# Tipos que solo se concatenan
CONCATENATE_ONLY = ['station', 'member_type']

# Función de consolidación
def consolidate_files(data_type):
    files = [f for f in os.listdir(GOLD_FOLDER) if f.startswith(data_type) and f.endswith(".parquet")]

    print(f"\n📄 Consolidando tipo '{data_type}': {len(files)} archivos")

    if not files:
        print(f"⚠️ No se encontraron archivos para '{data_type}'")
        return

    all_data = []
    for file in files:
        file_path = os.path.join(GOLD_FOLDER, file)
        try:
            df = pd.read_parquet(file_path)
            all_data.append(df)
        except Exception as e:
            print(f"❌ Error leyendo {file}: {e}")

    consolidated_data = pd.concat(all_data, ignore_index=True)

    if data_type in CONCATENATE_ONLY:
        final_data = consolidated_data
        print(f"🔗 Solo concatenado (sin agrupación) para '{data_type}'.")

    else:
        # Claves por tipo
        keys_dict = {
            'daily': ['date'],
            'monthly': ['year_month'],
            'hourly': ['year_month', 'start_hour'],
            'yearly': ['year']
        }

        keys = keys_dict.get(data_type)
        if keys and all(col in consolidated_data.columns for col in keys):
            print(f"🔗 Agrupando por {keys}")
            final_data = consolidated_data.groupby(keys, dropna=False).sum(numeric_only=True).reset_index()
        else:
            print(f"⚠️ No se pueden usar claves para '{data_type}' (faltan columnas). Usando concatenación.")
            final_data = consolidated_data

    # Guardar
    output_file = f"{data_type}_consolidated.parquet"
    output_path = os.path.join(CONSOLIDATED_FOLDER, output_file)
    final_data.to_parquet(output_path, index=False)
    print(f"✅ Guardado: {output_path}")

def consolidate_all():
    data_types = ['daily', 'monthly', 'station', 'member_type', 'hourly', 'yearly']
    for data_type in data_types:
        consolidate_files(data_type)
    print("\n🏁 Consolidación completada.")

if __name__ == "__main__":
    consolidate_all()
