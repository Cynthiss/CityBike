from azure.storage.blob import BlobServiceClient

# La cadena de conexión de tu cuenta de almacenamiento
connection_string = "DefaultEndpointsProtocol=https;AccountName=datacitybikes;AccountKey=sOzQFreiy/HgrkS0Eu3m3+YfP2x7bvu8syWnKedSHtRfIOjG8r/mxFwo5mfwlvrfuDKjIrXOq98h+AStulbRng==;EndpointSuffix=core.windows.net"

# Crear el BlobServiceClient usando la cadena de conexión
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Seleccionar el contenedor
container_name = "gold"  # Tu contenedor
container_client = blob_service_client.get_container_client(container_name)

# Listar los blobs (archivos) en el contenedor
print(f"Archivos en el contenedor '{container_name}':")
blobs = container_client.list_blobs()

# Mostrar los nombres de los archivos
for blob in blobs:
    print(blob.name)
