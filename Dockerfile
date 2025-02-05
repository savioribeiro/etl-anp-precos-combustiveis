# Usar uma imagem base do Python
FROM python:3.10-slim

# Definir diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar os arquivos do projeto para o contêiner
COPY . .

# Instalar dependências
RUN pip install -r requirements.txt

# Executar ETL ao iniciar o contêiner
CMD ["python", "main.py"]
