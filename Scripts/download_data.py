import os
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://s3.amazonaws.com/tripdata/"
DEST_FOLDER = "Data/Raw/"

def download_all_zips():
    os.makedirs(DEST_FOLDER, exist_ok=True)
    
    print("🔍 Obteniendo lista de archivos...")
    page = requests.get(BASE_URL)

    # Usamos parser XML porque la página es XML, no HTML
    soup = BeautifulSoup(page.content, 'lxml-xml')

    links = [key.text for key in soup.find_all('Key') if key.text.endswith('.zip')]

    print(f"📄 Archivos encontrados: {len(links)}")

    for link in links:
        filename = link.split('/')[-1]
        
        # Omitir archivos que contienen "JC" en el nombre
        if "JC" in filename:
            print(f"🚫 {filename} omitido por contener 'JC' en el nombre.")
            continue

        path = os.path.join(DEST_FOLDER, filename)
        
        if not os.path.exists(path):
            print(f"⬇️ Descargando {filename}...")
            response = requests.get(BASE_URL + filename, stream=True)
            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
            print(f"✅ Guardado: {filename}")
        else:
            print(f"🟢 {filename} ya existe, se omitió.")

if __name__ == "__main__":
    download_all_zips()
