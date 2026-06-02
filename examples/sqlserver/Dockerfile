FROM python:3.14-slim

WORKDIR /app

# Install msodbcsql18 driver (OS-level dependency for pyodbc)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml poetry.lock README.md LICENSE ./
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install -E all --no-root --no-interaction --no-ansi && \
    pip install --no-cache-dir psycopg2-binary pymysql clickhouse-sqlalchemy && \
    pip uninstall -y poetry

# Copy application code and install project
COPY slayer/ slayer/
RUN pip install --no-deps . && \
    useradd --create-home slayer
USER slayer

ENV SLAYER_MODELS_DIR=/data
EXPOSE 5143

CMD ["slayer", "serve", "--host", "0.0.0.0", "--port", "5143", "--models-dir", "/data"]
