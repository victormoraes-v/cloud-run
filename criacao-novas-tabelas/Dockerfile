# -----------------------------
# Base Python
# -----------------------------
FROM python:3.10-slim-bullseye

# -----------------------------
# System dependencies
# -----------------------------
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gcc \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Oracle Instant Client
# -----------------------------
WORKDIR /opt/oracle

RUN wget https://download.oracle.com/otn_software/linux/instantclient/2113000/instantclient-basiclite-linux.x64-21.13.0.0.0dbru.zip \
    && unzip instantclient-basiclite-linux.x64-21.13.0.0.0dbru.zip \
    && rm *.zip \
    && echo "/opt/oracle/instantclient_21_13" > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

# -----------------------------
# Environment variables
# -----------------------------
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_13
ENV ORACLE_HOME=/opt/oracle/instantclient_21_13

# -----------------------------
# App setup
# -----------------------------
WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# -----------------------------
# Cloud Run (functions-framework)
# -----------------------------
CMD ["functions-framework", "--source=main.py", "--target=main", "--port=8080"]