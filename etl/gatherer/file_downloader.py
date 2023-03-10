"""Module for download missing AIS files on demand."""
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
    """
    Class representing an AIS file.

    Attributes
    ----------
    name: name of the file
    url: link to download file
    """

    name: str
    url: str


def ensure_file_for_date(date: datetime, config) -> str:
    """
    Ensure that the file for the given date exists and return the file path.

    Raises exception if file has not already been downloaded and cannot be found on AIS website.

    Keyword arguments:
        date: the date to ensure the file for
        config: the application configuration
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
    Extract and return date information from a given filename.

    Keyword arguments:
        file_name: the name of the file used to extract date information

    Examples
    --------
    >>> date_from_filename("aisdk-2007-04.zip")
    datetime(2007, 4, 1)

    >>> date_from_filename("aisdk-2007-04-03.zip")
    datetime(2007, 4, 3)
    """
    file_name = file_name.replace('aisdk-', '').replace('.zip', '').replace('.rar', '')
    file_name = file_name.split('-')

    year = int(file_name[0])
    month = int(file_name[1])

    # If the file name does not contain a day, assume the first day of the month.
    day = int(file_name[2]) if len(file_name) == 3 else 1

    return datetime(year=year, month=month, day=day)


def get_file_names(ais_url: str) -> dict:
    """
    Fetch and return AIS filenames from webpage.

    Keyword arguments:
        ais_url: url to html page containing AIS archieves hyperlinks
    """
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


def extract(file: AisFile, config):
    """
    Extract the AIS file from its archieve.

    Keyword arguments:
        file: name and url of the AIS file
    """
    path = os.path.join(config['DataSource']['ais_path'], file.name)
    if path.endswith('.zip'):
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(config['DataSource']['ais_path'])
    elif path.endswith('.rar'):
        patoolib.extract_archive(path, config['DataSource']['ais_path'])


def ensure_file(file: AisFile, config):
    """
    Ensure that the AIS file is present in the file system.

    Downloads the file if not found in file system.

    Keyword arguments:
        file: name and url of a AIS file
        config: the application configuration
    """
    # Download the file if it does not exist.
    path = os.path.join(config['DataSource']['ais_path'], file.name)
    if not os.path.isfile(path):
        wrap_with_timings(f"Downloading file: {file.name}", lambda: wget.download(file.url, out=path))
    else:
        print(f"File already exists: {path}")
