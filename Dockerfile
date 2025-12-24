# --------------------------------------------------------
# 1. BASE IMAGE
# --------------------------------------------------------
FROM python:3.12-slim-bookworm

LABEL maintainer="youngjoo"
LABEL version="0.0.1"
LABEL description="data-engineering template docker image"
# --------------------------------------------------------
# 2. SYSTEM DEPENDENCIES
# --------------------------------------------------------
ENV DEBIAN_FRONTEND=noninteractive
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8


RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    # Optional: Add build-essential if you have C-extensions (numpy/pandas usually possess wheels, so this is often optional)
    # build-essential \
    && rm -rf /var/lib/apt/lists/*

# --------------------------------------------------------
# 3. INJECT UV
# --------------------------------------------------------
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# --------------------------------------------------------
# 4. ENVIRONMENT CONFIGURATION
# --------------------------------------------------------
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV UV_COMPILE_BYTECODE=1
ENV UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV
ENV UV_LINK_MODE=copy

# --------------------------------------------------------
# 5. DEPENDENCY INSTALLATION
# --------------------------------------------------------
WORKDIR /app

ARG INSTALL_GROUPS="--group data --group db --group eda"

COPY pyproject.toml uv.lock ./

# Optimization:
# Since we are using the official Python image, we don't need to install Python.
# We just sync the libraries.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project $INSTALL_GROUPS

# --------------------------------------------------------
# 6. RUNTIME
# --------------------------------------------------------
# Note: No CMD here. We define the command in docker-compose.
# This makes the image reusable for workers, web servers, or dev shells.