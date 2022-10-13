
import sys
import argparse
import configparser
import os

from time import perf_counter
from datetime import datetime, timedelta

from etl.helper_functions import wrap_with_timings
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


def main(argv):
    config = get_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--init", help="Initialize database", action="store_true")
    parser.add_argument("--load", help="Perform loading of the dates", action="store_true")
    parser.add_argument("--file", help="File to load")
    args = parser.parse_args()

    if args.init:
        wrap_with_timings("Database init", lambda: init_database(config))

    if args.load:
        if args.file is None or not os.path.isfile(os.path.join(config['DataSource']['ais_path'], args.file)):
            print("Please specify a valid file to load")
            exit(2)

        wrap_with_timings("Loading", lambda: load_data(config))

if __name__ == '__main__':
    main(sys.argv[1:])
