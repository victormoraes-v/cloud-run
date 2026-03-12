# Etapa 1: Imagem base oficial do Python
FROM python:3.10-slim

# Diretório de trabalho dentro do contêiner
WORKDIR /app

# Etapa 2: Copie o arquivo de dependências primeiro (melhora cache)
COPY requirements.txt ./

# Etapa 3: Instale dependências
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Etapa 4: Copie o restante do código da aplicação
COPY . .

# Etapa 5: Defina o comando para rodar o Job
# Agora chamamos diretamente o Python, sem functions-framework.
CMD ["python", "main.py"]
