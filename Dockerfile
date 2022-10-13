FROM python:3.10.7-slim-buster

RUN apt-get update && apt-get install -y gdal-bin python-gdal python3-gdal
RUN apt-get install -y postgresql-client libpq-dev

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .