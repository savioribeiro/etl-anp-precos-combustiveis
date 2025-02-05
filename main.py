from concurrent.futures import ThreadPoolExecutor
from extract import download_files, delete_existing_files
from transform import process_file
from load import load_to_postgres
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%d-%m-%Y %H:%M:%S')
logg = logging.getLogger(__name__)

FILE_LINKS = os.getenv("FILE_LINKS").split(',')

def main():

    # Delete existing files in /data/
    delete_existing_files()

    # Extract
    files = download_files(FILE_LINKS)

    # Transform
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(process_file, files))

    # Load
    logg.info("Iniciando a carga dos dados no BD.")
    for i, (df, filepath) in enumerate(results):
        logg.info(f"Carregando o df {i+1} com {len(df)} linhas.")
        load_to_postgres(df)

    logg.info(f"ETL concluída.")

if __name__ == "__main__":
    main()