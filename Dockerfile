FROM python:3.11.2-slim-bullseye

RUN apt-get update && apt-get install -y \
    gdal-bin \
    python3-gdal \
    postgresql-client \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /python
WORKDIR /python

COPY requirements.txt .

RUN pip install -r requirements.txt

ARG tag

ENV tag=$tag

COPY . .
