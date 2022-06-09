FROM python:3.8-slim-bullseye as base

# Metadata
LABEL org.label-schema.vendor="ScaleVector" \
    org.label-schema.url="https://scalevector.ai" \
    org.label-schema.name="dbt_runner" \
    org.label-schema.description="DBT Package Runner for DLT"

# prepare dirs to install autopoieses
RUN mkdir -p /usr/src/app && mkdir /var/local/app && mkdir /usr/src/app/dbt_runner

WORKDIR /usr/src/app

# System setup for DBT
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

ENV PYTHONPATH $PYTHONPATH:/usr/src/app

# add build labels and envs
ARG COMMIT_SHA=""
ARG IMAGE_VERSION=""
ARG DLT_VERSION=""
LABEL commit_sha = ${COMMIT_SHA}
LABEL version=${IMAGE_VERSION}
LABEL dlt_version=${DLT_VERSION}
ENV COMMIT_SHA=${COMMIT_SHA}
ENV IMAGE_VERSION=${IMAGE_VERSION}
ENV DLT_VERSION=${DLT_VERSION}

# install exactly the same version of the library we used to build
RUN pip3 install python-dlt[gcp,redshift]==${DLT_VERSION}
