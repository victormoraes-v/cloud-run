# Etapa 1: Use uma imagem base oficial do Python. A versão "slim" é menor.
# Use a mesma versão do Python que você usa para desenvolver.
FROM python:3.10-slim

# Define a variável de ambiente PORT, que o Cloud Run usa para enviar requisições.
ENV PORT 8080

# Define um diretório de trabalho dentro do contêiner.
WORKDIR /app

# Etapa 2: Copie APENAS o arquivo de dependências primeiro.
# Isso aproveita o cache de camadas do Docker. Se o requirements.txt não mudar,
# o Docker não precisará reinstalar as dependências toda vez que você reconstruir a imagem.
COPY requirements.txt ./

# Etapa 3: Instale as dependências.
# --no-cache-dir cria uma imagem menor.
# --upgrade pip garante que estamos usando a versão mais recente do pip.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Etapa 4: Copie o restante do código da sua aplicação para o diretório de trabalho.
COPY . .

# Etapa 5: Comando para iniciar a aplicação usando Gunicorn.
# - 'exec' garante que o Gunicorn se torne o processo principal (PID 1).
# - 'gunicorn' é o servidor.
# - '-w 4' (workers): Inicia 4 processos para lidar com requisições. Um bom ponto de partida.
# - '-b :$PORT' (bind): Escuta na porta definida pela variável de ambiente PORT.
#   Neste caso, o Functions Framework cria um objeto 'app' por trás dos panos.
#   Para funções HTTP, o padrão é usar o nome da função.
#   IMPORTANTE: O Functions Framework espera ser o ponto de entrada.
#   A maneira correta de usá-lo com um servidor de produção é especificar o --target.
CMD exec functions-framework --target=main --host=0.0.0.0 --port=$PORT