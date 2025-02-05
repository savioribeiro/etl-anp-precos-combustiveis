import os
import psycopg2
import pandas as pd
import logging
from dotenv import load_dotenv
from psycopg2 import sql
import psycopg2.extras
import unicodedata
import re

load_dotenv()

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%d-%m-%Y %H:%M:%S')
logg = logging.getLogger(__name__)

DB_URL = os.getenv("DB_URL")
SCHEMA = os.getenv("DB_SCHEMA")

def normalize_column_name(name):
    name = ''.join(c for c in unicodedata.normalize('NFKD', name) if not unicodedata.combining(c))
    name = re.sub(r'\s+', '_', name).lower()
    return name

def load_to_postgres(df):
    try:
        df.columns = [normalize_column_name(col) for col in df.columns]
        
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        cursor.execute(sql.SQL(f'''
            CREATE SCHEMA IF NOT EXISTS {SCHEMA};
        '''))
        
        cursor.execute(sql.SQL(f'''
            CREATE TABLE IF NOT EXISTS {SCHEMA}.precos_combustiveis (
                data_inicial DATE,
                data_final DATE,
                regiao TEXT,
                estado TEXT,
                municipio TEXT,
                produto TEXT,
                numero_de_postos_pesquisados INTEGER,
                unidade_de_medida TEXT,
                preco_medio_revenda FLOAT,
                preco_minimo_revenda FLOAT,
                preco_maximo_revenda FLOAT,
                UNIQUE (data_inicial, estado, municipio, produto)
            );
        '''))
        
        insert_query = sql.SQL(f'''
            INSERT INTO {SCHEMA}.precos_combustiveis (
                data_inicial, data_final, regiao, estado, municipio, produto,
                numero_de_postos_pesquisados, unidade_de_medida, preco_medio_revenda,
                preco_minimo_revenda, preco_maximo_revenda
            ) VALUES %s
            ON CONFLICT (data_inicial, estado, municipio, produto) DO NOTHING;
        ''')
        
        psycopg2.extras.execute_values(
            cursor, insert_query, df.values, template=None, page_size=100
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logg.info(f"Dados carregados no PostgreSQL com sucesso.")
    except Exception as e:
        logg.error(f"Erro ao carregar dados no PostgreSQL: {e}")
