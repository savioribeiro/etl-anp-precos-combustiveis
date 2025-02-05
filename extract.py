import os
import requests
import logging
import glob

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%d-%m-%Y %H:%M:%S')
logg = logging.getLogger(__name__)

DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def delete_existing_files():
    files = glob.glob(os.path.join(DOWNLOAD_DIR, '*.xlsb')) + glob.glob(os.path.join(DOWNLOAD_DIR, '*.xlsx'))
    for file in files:
        os.remove(file)
        logg.info(f"Arquivo deletado: {file}")

def download_files(file_links):

    files = []

    for url in file_links:
        filename = os.path.join(DOWNLOAD_DIR, url.split('/')[-1])
        if not os.path.exists(filename):
            logg.info(f"Iniciando download do arquivo: {filename}")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logg.info(f"Download concluído: {filename}")

        files.append(filename)

    logg.info("Todos os arquivos foram baixados.")

    return files