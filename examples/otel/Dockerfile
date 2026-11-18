# SLayer for this example, with only the extras it needs: Postgres and OpenTelemetry.
#
# Why this file exists: the other examples build from the root Dockerfile, but that file
# does not build on ARM Linux (Apple Silicon, AWS Graviton). It installs every extra,
# including clickhouse-sqlalchemy, which needs zstd. PyPI has no ARM Linux wheels for
# zstd, so pip compiles it from source, and the slim base image has no C compiler.
# This file installs only the postgres and otel extras, so it never builds zstd.
#
# Remove this file, and build from the root Dockerfile, after the root Dockerfile builds
# on ARM. The root image installs "-E all", which includes the otel extra.
FROM python:3.12-slim

WORKDIR /app

# The versions come from poetry.lock. Poetry builds SLayer itself from the copied source.
COPY pyproject.toml poetry.lock README.md LICENSE ./
COPY slayer/ slayer/
RUN pip install --no-cache-dir --only-binary :all: "poetry==2.5.1" && \
    poetry config virtualenvs.create false && \
    poetry install --only main -E postgres -E otel --no-interaction --no-ansi && \
    pip uninstall -y poetry

# Run as a non-root user that owns the storage volume.
RUN useradd --create-home slayer && mkdir /data && chown slayer /data
USER slayer

EXPOSE 5143
