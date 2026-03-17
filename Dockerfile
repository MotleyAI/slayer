FROM python:3.14-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml poetry.lock* README.md LICENSE ./
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install -E all --no-root --no-interaction --no-ansi && \
    pip install --no-cache-dir psycopg2-binary pymysql clickhouse-sqlalchemy && \
    pip uninstall -y poetry

# Copy application code and install project
COPY slayer/ slayer/
RUN pip install --no-deps .

# Run as non-root user
RUN useradd --create-home slayer
USER slayer

ENV SLAYER_MODELS_DIR=/data
EXPOSE 5143

CMD ["slayer", "serve", "--host", "0.0.0.0", "--port", "5143", "--models-dir", "/data"]
