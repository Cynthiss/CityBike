from azure.storage.blob import BlobServiceClient
import os
# :marca_de_verificación_blanca: Cadena de conexión (coloca la tuya aquí correctamente)
connection_string = "DefaultEndpointsProtocol=https;AccountName=datacitybikes;AccountKey=xxxxxxxxxxxxxxxxxxxxxxxxxxxx;EndpointSuffix=core.windows.net"
# :marca_de_verificación_blanca: Lista de contenedores (debe coincidir con las carpetas locales)
containers = ["raw", "bronze", "silver", "gold"]
# :marca_de_verificación_blanca: Ruta base local donde están las carpetas de cada contenedor
base_local_path = r"C:\Users\Santos\Documents\GitHub\CityBike\Data"
def upload_to_container(container_name):
    local_directory = os.path.join(base_local_path, container_name)
    if not os.path.exists(local_directory):
        print(f":x: No existe la carpeta local '{local_directory}'. Saltando...")
        return
    try:
        # Cliente
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        # Crear contenedor si no existe
        try:
            container_client.create_container()
            print(f":cohete: Contenedor '{container_name}' creado.")
        except Exception:
            print(f":fuente_de_información: Contenedor '{container_name}' ya existe.")
        # Subir archivos
        for root, dirs, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_directory)
                blob_path = relative_path.replace("\\", "/")
                blob_client = container_client.get_blob_client(blob_path)
                with open(local_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print(f":marca_de_verificación_gruesa: Subido: {blob_path} a contenedor '{container_name}'.")
    except Exception as e:
        print(f":x: Error subiendo a contenedor '{container_name}': {e}")
def main():
    for container in containers:
        upload_to_container(container)
if __name__ == "__main__":
    main()