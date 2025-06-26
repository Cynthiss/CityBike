# --------------------------------
# --------------------- SUBIR UN ARCHIVO EN ESPECIFICO ----------------------------

# from azure.storage.blob import BlobServiceClient

# # La cadena de conexión de tu cuenta de almacenamiento
# connection_string = "DefaultEndpointsProtocol=https;AccountName=datacitybikes;AccountKey=sOzQFreiy/HgrkS0Eu3m3+YfP2x7bvu8syWnKedSHtRfIOjG8r/mxFwo5mfwlvrfuDKjIrXOq98h+AStulbRng==;EndpointSuffix=core.windows.net"

# # Crear el BlobServiceClient usando la cadena de conexión
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# # Seleccionar el contenedor
# container_name = "data-cbike"  # Tu contenedor
# container_client = blob_service_client.get_container_client(container_name)

# # Ruta del archivo local
# local_file_path = "ruta/a/tu/archivo.csv"  # Reemplaza con la ruta a tu archivo
# blob_name = "archivo_en_azure.csv"  # Nombre que tendrá el archivo en Azure

# # Verificar si el archivo ya existe en el contenedor
# try:
#     container_client.get_blob_client(blob_name).get_blob_properties()
#     print(f"⏩ El archivo '{blob_name}' ya existe en el contenedor '{container_name}'. Saltando carga.")
# except:
#     # Subir el archivo si no existe
#     with open(local_file_path, "rb") as data:
#         container_client.upload_blob(blob_name, data)
#     print(f"Archivo '{blob_name}' subido exitosamente al contenedor '{container_name}'.")

#COMENTAR CONTROL + K + C
# DES COMENTAR CONTROL + K + U

# ------------------------------------------
# SUBIR DIRECTORIO COMPLETO
from azure.storage.blob import BlobServiceClient
import os

# Conexión
connection_string = "DefaultEndpointsProtocol=https;AccountName=datacitybikes;AccountKey=xxxxxxxxxxxxxxxx;EndpointSuffix=core.windows.net"

# Lista de contenedores (capas)
containers = ["raw", "bronze", "silver", "gold"]

# Ruta base local
base_local_path = r"C:\Users\Santos\Documents\GitHub\CityBike\Data"

def upload_to_container(container_name):
    local_directory = os.path.join(base_local_path, container_name)
    if not os.path.exists(local_directory):
        print(f"No existe carpeta local para '{container_name}'.")
        return

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Crea contenedor si no existe
    try:
        container_client.create_container()
        print(f"Contenedor '{container_name}' creado.")
    except Exception:
        print(f"ℹContenedor '{container_name}' ya existe.")

    # Upload
    for root, dirs, files in os.walk(local_directory):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_directory)
            blob_path = relative_path.replace("\\", "/")

            blob_client = container_client.get_blob_client(blob_path)
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            print(f"✔️ Subido: {blob_path}")

def main():
    for container in containers:
        upload_to_container(container)

if __name__ == "__main__":
    main()
