import os
from dataclasses import dataclass
from datetime import datetime
import wget
import zipfile
import patoolib
import requests
from bs4 import BeautifulSoup
from etl.helper_functions import wrap_with_timings


@dataclass
class AisFile:
    name: str
    url: str


def ensure_file_for_date(date: datetime, config) -> str:
    """
        Ensures that the file for the given date exists.
        If it does not exist, try every trick in the book to gather it. If it still does not exist, raise an exception.
    :param date: The date to ensure the file for.
    :param config:
    :return: The path to the file.
    """
    expected_filename = f"aisdk-{date.year}-{date.month:02d}-{date.day:02d}.csv"
    path = os.path.join(config['DataSource']['ais_path'], expected_filename)

    # First, check if the file exists.
    if os.path.isfile(path):
        print(f"File already exists: {path}")
        return path

    # The file does not exist, check what files are available from DMA.
    file_names = get_file_names(config['DataSource']['ais_url'])

    # Check if our current date is in the list of available files.
    # If not, check if the month is in the list of available files.
    if date not in file_names:
        print(f"File not found for date: {date}. Trying first day of month.")
        date = datetime(year=date.year, month=date.month, day=1)
        if date not in file_names:
            raise Exception(f"File for {date} not found as existing on Danish Maritime Authority website.")

    # The file exists, download it.
    file = file_names[date]
    ensure_file(file, config)
    extract(file, config)

    # In case the downloaded file did not actually contain the desired date, raise an exception.
    if not os.path.isfile(os.path.join(config['DataSource']['ais_path'], expected_filename)):
        raise Exception(f"Expected file {expected_filename} was not found in {config['DataSource']['ais_path']}")

    return path


def date_from_filename(file_name):
    """
        Eg "aisdk-2007-04.zip" -> datetime(2007, 4, 1), "aisdk-2007-04-03.zip" -> datetime(2007, 4, 3)
    :param file_name:
    :return:
    """
    file_name = file_name.replace('aisdk-', '').replace('.zip', '').replace('.rar', '')
    file_name = file_name.split('-')

    year = int(file_name[0])
    month = int(file_name[1])

    # If the file name does not contain a day, assume the first day of the month.
    day = int(file_name[2]) if len(file_name) == 3 else 1

    return datetime(year=year, month=month, day=day)


def get_file_names(ais_url: str) -> dict:
    # Fetch the HTML from DMA.
    response = requests.get(ais_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    # Find all links in the HTML.
    anchors = soup.find_all('a')
    ais_files = {}

    for anchor in anchors:
        name = anchor['href']
        url = ais_url + name

        # Skip if name is not zip or rar
        if not name.endswith('.zip') and not name.endswith('.rar'):
            continue

        ais_file = AisFile(name, url)
        ais_files[date_from_filename(name)] = ais_file
    return ais_files


def extract(file, config):
    path = os.path.join(config['DataSource']['ais_path'], file.name)
    if path.endswith('.zip'):
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(config['DataSource']['ais_path'])
    elif path.endswith('.rar'):
        patoolib.extract_archive(path, config['DataSource']['ais_path'])


def ensure_file(file: AisFile, config):
    # Download the file if it does not exist.
    path = os.path.join(config['DataSource']['ais_path'], file.name)
    if not os.path.isfile(path):
        wrap_with_timings(f"Downloading file: {file.name}", lambda: wget.download(file.url, out=path))
    else:
        print(f"File already exists: {path}")
