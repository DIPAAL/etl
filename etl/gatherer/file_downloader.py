import os
from dataclasses import dataclass
from datetime import datetime
import wget
import zipfile
import patoolib
import requests
from bs4 import BeautifulSoup
from etl.helper_functions import wrap_with_timings

AIS_URL = "https://web.ais.dk/aisdata/"

@dataclass
class AisFile:
    name: str
    url: str
    size: int # number of bytes



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
    day = int(file_name[2]) if len(file_name) == 3 else 1

    return datetime(year=year, month=month, day=day)


def get_file_names():
    # Fetch the HTML from DMA.
    response = requests.get(AIS_URL)

    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all rows in the table.
    rows = soup.find_all('tr')

    # Create a dictionary from datetime to AisFile objects.
    ais_files = {}

    for row in rows:
        cols = row.find_all('td')
        if len(cols) == 5:
            # The second column contains the anchor
            anchor = cols[1].find('a')
            if anchor is None:
                continue

            name = anchor['href']
            url = AIS_URL + name

            # skip if name is not zip or rar
            if not name.endswith('.zip') and not name.endswith('.rar'):
                continue

            # Size is in the fourth column
            size_human_readable = cols[3].text
            # either measured in M or G for Mega or Giga bytes.
            size = int(float(size_human_readable[:-1]) * 1024 * 1024 if size_human_readable[-1] == 'M' else float(size_human_readable[:-1]) * 1024 * 1024 * 1024)

            ais_file = AisFile(name, url, size)
            ais_files[date_from_filename(name)] = ais_file
    return ais_files


def extract(file, config):
    path = os.path.join(config['DataSource']['ais_path'], file.name)
    if path.endswith('.zip'):
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(config['DataSource']['ais_path'])
    elif path.endswith('.rar'):
        patoolib.extract_archive(path, config['DataSource']['ais_path'])


def ensure_file_for_date(date: datetime, config) -> True:
    expected_filename = f"aisdk-{date.year}-{date.month:02d}-{date.day:02d}.csv"
    if os.path.isfile(os.path.join(config['DataSource']['ais_path'], expected_filename)):
        print(f"File already exists: {expected_filename}")
        return True

    # Get the file names from the web page.
    file_names = get_file_names()

    # First check if the file for the given date exists
    if not date in file_names:
        print(f"File not found for date: {date}. Trying first day of month.")
        # try the first day of the month
        date = datetime(year=date.year, month=date.month, day=1)
        if not date in file_names:
            print(f"File not found for date: {date}")
            return False

    if date in file_names:
        file = file_names[date]
        ensure_file(file, config)
        extract(file, config)
        if not os.path.isfile(os.path.join(config['DataSource']['ais_path'], expected_filename)):
            raise Exception(f"Expected file {expected_filename} was not found in {config['DataSource']['ais_path']}")

        return True

    raise Exception("No file for date: " + str(date))

def ensure_file(file: AisFile, config):
    # Download the file if it does not exist.
    path = os.path.join(config['DataSource']['ais_path'], file.name)
    if not os.path.isfile(path):
        wrap_with_timings(f"Downloading file: {file.name} ({file.size} bytes)" , lambda: wget.download(file.url, out=path))
    else:
        print(f"File already exists: {path}")

