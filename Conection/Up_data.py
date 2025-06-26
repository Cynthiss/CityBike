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

import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Cadena de conexión de tu cuenta de Azure Storage
connection_string = "DefaultEndpointsProtocol=https;AccountName=datacitybikes;AccountKey=sOzQFreiy/HgrkS0Eu3m3+YfP2x7bvu8syWnKedSHtRfIOjG8r/mxFwo5mfwlvrfuDKjIrXOq98h+AStulbRng==;EndpointSuffix=core.windows.net"
container_name = "data-cbike"  # Nombre del contenedor en Azure

def upload_directory(local_directory_path, container_name):
    # Crear el cliente de BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Recorrer todos los archivos en el directorio y subcarpetas
    for root, dirs, files in os.walk(local_directory_path):
        for file in files:
            # Crear la ruta del archivo completo
            local_file_path = os.path.join(root, file)
            
            # Convertir la ruta local a la ruta del blob
            relative_path = os.path.relpath(local_file_path, local_directory_path)
            blob_path = os.path.join("data", relative_path).replace("\\", "/")  # Para la estructura de carpetas en Azure

            # Subir el archivo al contenedor
            blob_client = container_client.get_blob_client(blob_path)
            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"Archivo subido: {blob_path}")

# Ruta local de la carpeta PARA SUBIR 
local_directory_path = r"C:\Users\Santos\Documents\GitHub\CityBike\Data\gold_p"  # Reemplaza con la ruta de tu directorio
upload_directory(local_directory_path, container_name)

#Subir directorio completo