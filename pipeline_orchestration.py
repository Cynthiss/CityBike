from prefect import flow, task
import subprocess

# Tarea 1: Descargar los archivos desde el sitio S3
@task
def download_data():
    print("⬇️ Descargando datos...")
    subprocess.run(["python", "Scripts/download_data.py"])

# Tarea 2: Extraer los archivos ZIP descargados
@task
def extract_data():
    print("📦 Extrayendo archivos...")
    subprocess.run(["python", "Scripts/extract_data.py"])

# Tarea 3: Convertir archivos CSV a formato Parquet (etapa Bronze)
@task
def bronze_conversion():
    print("🔁 Convirtiendo a Bronze...")
    subprocess.run(["python", "Scripts/bronze_trans.py"])

# Tarea 4: Limpiar y transformar los datos para la capa Silver
@task
def silver_transformation():
    print("✨ Transformando a Silver...")
    subprocess.run(["python", "Scripts/silver_transform.py"])

# Tarea 5: Calcular agregaciones para la capa Gold
@task
def gold_aggregations():
    print("📊 Generando capa Gold...")
    subprocess.run(["python", "Scripts/gold_transform.py"])

# Tarea 6: Exportar los Parquet Gold a CSV para visualización
#@task
#def export_to_csv():
    #print("📝 Exportando Gold a CSV...")
    #subprocess.run(["python", "Scripts/export_gold_to_csv.py"])


# Flujo principal que define el orden de las tareas
@flow(name="citybike_etl_pipeline")
def citybike_etl():
    download_data()
    extract_data()
    bronze_conversion()
    silver_transformation()
    gold_aggregations()

# Punto de entrada para ejecutar el flujo desde consola
if __name__ == "__main__":
    citybike_etl()

# COMANDO TERMINAL:
# python Scripts/pipeline_orchestration.py

