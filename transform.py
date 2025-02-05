import pandas as pd
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%d-%m-%Y %H:%M:%S')
logg = logging.getLogger(__name__)

HEADERS = os.getenv("HEADERS").split(',')

def process_file(filepath):
    try:
        logg.info(f"Processando o arquivo: {filepath}")

        file_extension = filepath.split('.')[-1].lower()
        
        if file_extension == 'xlsb':
            engine = 'pyxlsb'
        elif file_extension == 'xlsx':
            engine = 'openpyxl'
        else:
            raise ValueError(f"Formato de arquivo não suportado: {file_extension}")

        sheets = pd.read_excel(filepath, sheet_name=None, engine=engine, nrows=100)
        df_preview = list(sheets.values())[0]

        index_data_inicial = df_preview[ 
            df_preview.apply(lambda row: row.astype(str).str.contains('DATA INICIAL', na=False).any(), axis=1)
        ].index[0]

        df_full = pd.read_excel(
            filepath,
            sheet_name=None,
            engine=engine,
            skiprows=index_data_inicial,
            header=1
        )
        df = list(df_full.values())[0]

        df[['PREÇO MÉDIO REVENDA', 'PREÇO MÍNIMO REVENDA', 'PREÇO MÁXIMO REVENDA']] = df[['PREÇO MÉDIO REVENDA', 'PREÇO MÍNIMO REVENDA', 'PREÇO MÁXIMO REVENDA']].round(3)

        if filepath.endswith('.xlsb'):
            df['DATA INICIAL'] = pd.to_datetime(df['DATA INICIAL'], unit='D', origin='1899-12-30', errors='coerce').dt.date
            df['DATA FINAL'] = pd.to_datetime(df['DATA FINAL'], unit='D', origin='1899-12-30', errors='coerce').dt.date
        else:
            df['DATA INICIAL'] = pd.to_datetime(df['DATA INICIAL'], errors='coerce').dt.date
            df['DATA FINAL'] = pd.to_datetime(df['DATA FINAL'], errors='coerce').dt.date

        logg.info(f"O arquivo foi processado: {filepath}")
        return df[HEADERS], filepath

    except Exception as e:
        logg.error(f"Erro ao processar {filepath}: {e}")
        return pd.DataFrame(columns=HEADERS), filepath