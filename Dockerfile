# A docker container with neo4j, java and python for ORION

# Stage 1: Source Neo4j binaries from the official image
FROM neo4j:5.25.1-community-bullseye AS neo4j-source

# Stage 2: Build on Python 3.12
FROM python:3.12-bookworm

# Install Java 17 (required by Neo4j) and other utilities
RUN apt-get update \
    && apt-get -y install openjdk-17-jre-headless git vim \
    && rm -rf /var/lib/apt/lists/*

# Create neo4j user/group matching the official image (UID/GID 7474)
RUN groupadd -r -g 7474 neo4j && useradd -r -u 7474 -g neo4j -m -d /var/lib/neo4j neo4j

# Copy Neo4j installation from the official image
COPY --from=neo4j-source /var/lib/neo4j /var/lib/neo4j
COPY --from=neo4j-source /startup /startup
ENV NEO4J_HOME="/var/lib/neo4j" \
    PATH="/var/lib/neo4j/bin:${PATH}"

# Create data/logs directories with symlinks matching the official Neo4j image layout
RUN mkdir -p /data /logs /var/lib/neo4j/run \
    && ln -sf /data /var/lib/neo4j/data \
    && ln -sf /logs /var/lib/neo4j/logs \
    && chmod -R 777 /var/lib/neo4j /data /logs

EXPOSE 7474 7473 7687

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /uvx /usr/local/bin/

# Use the image's Python, skip dev dependencies, and keep the venv outside /ORION
# so it isn't hidden when docker compose mounts the repo over /ORION
ENV UV_PYTHON_DOWNLOADS=0 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /ORION

# Install the locked dependencies (including optional robokop deps) in their own layer
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --extra robokop --no-install-project

COPY . /ORION/.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --extra robokop

RUN chmod -R 777 /ORION

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONPATH="$PYTHONPATH:/ORION"