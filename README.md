# ETL para Preços de Combustíveis da ANP

Pipeline de ETL para dados de preços de combustíveis da pesquisa da ANP - Agência Nacional do Petróleo. Os dados são baixados de links fornecidos previamente no `.env`, processados e carregados em um banco de dados PostgreSQL.

Por padrão, são usados os links com dados do intervalo abaixo:
- 2018 a 2021; e
- 2024 a 2025.

A pesquisa semanal da ANP inclui o preço dos seguintes combustíveis:

- gasolina comum, etanol hidratado, óleo diesel não aditivado, gás natural veicular (GNV) e gás GLP (gás de cozinha, 13kg).

## Estrutura do Projeto

- `main.py`: Script principal para executar o ETL.
- `dags/dag.py`: DAG do Airflow para agendar e executar o ETL.
- `docker-compose.yml`: Arquivo para configurar e executar o projeto usando Docker.
- `extract.py`: Funções para baixar e deletar arquivos.
- `transform.py`: Funções para processar os arquivos baixados.
- `load.py`: Funções para carregar os dados processados no PostgreSQL.
- `.env.example`: Arquivo de configuração com variáveis de ambiente.

## Requisitos

- Python 3.8+
- PostgreSQL
- Docker (opcional)
- Airflow (opcional)

## Instalação

1. Clone o repositório:
    ```sh
    git clone https://github.com/savioribeiro/etl-anp-precos-combustiveis.git
    cd etl-anp-precos-combustiveis
    ```

2. Crie um ambiente virtual e instale as dependências:
    ```sh
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3. Configure as variáveis de ambiente no arquivo `.env`:
    ```dotenv
    DB_URL=postgresql://user:password@localhost:5432/etl_combustiveis
    DB_SCHEMA=public
    HEADERS="DATA INICIAL,DATA FINAL,REGIÃO,ESTADO,MUNICÍPIO,PRODUTO,NÚMERO DE POSTOS PESQUISADOS,UNIDADE DE MEDIDA,PREÇO MÉDIO REVENDA,PREÇO MÍNIMO REVENDA,PREÇO MÁXIMO REVENDA"
    FILE_LINKS="https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal/semanal-municipios-2018-a-2021.xlsb, https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal/semanal-municipios-2024-2025.xlsx"
    ```

Você pode encontrar links com outros relatórios no site da [ANP](https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/serie-historica-do-levantamento-de-precos).
** Essa ETL está preparada para lidar somente com as **séries históricas semanais**. Ainda não é possível utilizar os microdados disponibilizados pela ANP.

## Executando o ETL

### Usando `main.py`

1. Execute o script principal:
    ```sh
    python main.py
    ```

### Usando Airflow

1. Configure o Airflow e adicione a DAG `dags/dag.py`.
2. Inicie o Airflow e agende a DAG.

![Airflow Graph](img/graph-etl.png)

### Usando Docker

1. Execute o Docker Compose:
    ```sh
    docker-compose up --build
    ```

Após o *build*, a ETL será executada, o container *app* será pausado após a conclusão e o *db* seguirá ativo.

2. Acesse o banco de dados PostgreSQL:
    ```sh
    docker exec -it etl_combustiveis_db psql -U user -d etl_combustiveis
    ```

3. Execute uma consulta simples:
    ```sql
    SELECT * FROM public.precos_combustiveis LIMIT 10;
    ```
![Airflow Graph](img/etl-simple-query.png)

## Variáveis de Ambiente

- `DB_URL`: URL de conexão com o banco de dados PostgreSQL.
- `DB_SCHEMA`: Esquema do banco de dados PostgreSQL.
- `HEADERS`: Cabeçalhos das colunas dos arquivos de dados.
- `FILE_LINKS`: Links para download dos arquivos de dados.

## Dicionário de Dados

| **Campo**                    | **Descrição**                                                                 |
|------------------------------|-------------------------------------------------------------------------------|
| `data_inicial`                | Data de início do período de coleta dos preços.                              |
| `data_final`                  | Data de término do período de coleta dos preços.                             |
| `regiao`                      | Região geográfica (Norte, Sul, etc.).                                        |
| `estado`                      | Estado da unidade federativa.                                                |
| `municipio`                   | Nome do município.                                                           |
| `produto`                     | Tipo de produto (por exemplo, Etanol Hidratado, Gasolina).                   |
| `numero_de_postos_pesquisados`| Quantidade de postos de combustíveis pesquisados na localidade.              |
| `unidade_de_medida`           | Unidade de medida para o preço (ex.: R$/l).                                  |
| `preco_medio_revenda`         | Preço médio de revenda do combustível.                                        |
| `preco_minimo_revenda`        | Preço mínimo encontrado para o combustível.                                  |
| `preco_maximo_revenda`        | Preço máximo encontrado para o combustível.                                  |

Embora não tenham sido adicionadas por padrão, as colunas abaixo também estão disponíveis:
- DESVIO PADRÃO REVENDA
- MARGEM MÉDIA REVENDA
- COEF DE VARIAÇÃO REVENDA

## Adicionando Novos Links

1. Adicione os novos links no arquivo `.env` na variável `FILE_LINKS`, separados por vírgula:
    ```dotenv
    FILE_LINKS="link1, link2, link3"
    ```

## Links Úteis

- [Metodologia da pesquisa e cidades contempladas](https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/arquivos-metodologia/metodologia-coleta.pdf)
- [Metadados Pesquisa de Preço ANP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/metadados-serie-historica-precos-combustiveis-1.pdf)
