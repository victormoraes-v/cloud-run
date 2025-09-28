# Python enxuto, suficiente para rodar requests/pandas/pyarrow
FROM python:3.11-slim

# Diretório de trabalho
WORKDIR /app

# Instala apenas dependências Python (mais rápido e previsível)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código
COPY . .

# Comando padrão do Job
CMD ["python", "main.py"]
