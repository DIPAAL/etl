
import sys
import argparse
import configparser
import os

from time import perf_counter
from datetime import datetime, timedelta

from etl.init_database import init_database
from etl.load_data import load_data


def get_config():
    path = './config.properties'
    local_path = './config-local.properties'
    if os.path.isfile(local_path):
        path = local_path
    config = configparser.ConfigParser()
    config.read(path)

    return config

def wrap_in_timings(name: str, func) -> None:
    print(f"{name} started at {datetime.now()}")
    start = perf_counter()
    func()
    end = perf_counter()
    print(f"{name} finished at {datetime.now()}")
    print(f"{name} took {timedelta(seconds=(end - start))}")


def main(argv):
    config = get_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--init", help="Initialize database", action="store_true")
    parser.add_argument("--load", help="Perform loading of the dates", action="store_true")
    args = parser.parse_args()

    if args.init:
        wrap_in_timings("Database init", lambda: init_database(config))

    if args.load:
        wrap_in_timings("Loading", lambda: load_data(config))

if __name__ == '__main__':
    main(sys.argv[1:])
