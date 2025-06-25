import os
import zipfile
import glob

RAW_FOLDER = "data/raw/"

def extract_all_zips():
    # Usar os.walk() para recorrer subcarpetas también
    zip_files = []
    for root, _, files in os.walk(RAW_FOLDER):
        zip_files.extend([os.path.join(root, file) for file in files if file.endswith(".zip")])

    print(f"🔍 Archivos ZIP encontrados: {len(zip_files)}")

    for zip_path in zip_files:
        try:
            # Obtener el nombre base del archivo
            file_name = os.path.basename(zip_path)
            expected_name = file_name.replace(".zip", "").lower()

            # Buscar si ya hay un CSV con ese nombre base
            matching_files = glob.glob(os.path.join(RAW_FOLDER, f"{expected_name}*.csv"))

            if matching_files:
                print(f"⏭️ Ya extraído (o duplicado): {file_name}")
                continue

            # Extraer ZIP completo
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(RAW_FOLDER)
                print(f"✅ Extraído: {file_name}")

        except zipfile.BadZipFile:
            print(f"⚠️ Archivo corrupto o inválido: {file_name}")

    # Eliminar archivos duplicados
    cleanup_duplicates()

    print("🎉 Extracción y limpieza completadas.")

def cleanup_duplicates():
    csv_files = glob.glob(os.path.join(RAW_FOLDER, "*.csv"))
    seen = {}

    for file in csv_files:
        base_name = os.path.basename(file).lower().replace(" (1)", "").replace(" (2)", "")
        if base_name in seen:
            os.remove(file)
            print(f"🗑️ Eliminado duplicado: {file}")
        else:
            seen[base_name] = file

if __name__ == "__main__":
    extract_all_zips()

