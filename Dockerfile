FROM python:3.10.9-slim-bullseye

RUN apt-get update && apt-get install -y gdal-bin python-gdal python3-gdal
RUN apt-get install -y postgresql-client libpq-dev

RUN mkdir /python
WORKDIR /python

COPY requirements.txt .

RUN pip install -r requirements.txt

ARG tag

ENV tag=$tag

COPY . .
