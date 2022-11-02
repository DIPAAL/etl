
import sys
import argparse
import configparser
import os
from datetime import datetime

import pandas as pd

from etl.gatherer.file_downloader import ensure_file_for_date
from etl.helper_functions import wrap_with_timings
from etl.init_database import init_database
from etl.cleaning.clean_data import clean_data
from etl.insert.insert_trajectories import TrajectoryInserter
from etl.trajectory.builder import build_from_geopandas


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
    parser.add_argument("--clean", help="Clean the given AIS data file", action="store_true")
    parser.add_argument("--date", help="The date to load, in the format YYYY-MM-DD, for example 2022-12-31", type=str)

    args = parser.parse_args()

    date = datetime.strptime(args.date, '%Y-%m-%d') if args.date else None

    if args.init:
        wrap_with_timings("Database init", lambda: init_database(config))

    if args.clean:

        file_path = wrap_with_timings(
            "Ensuring file for current date exists",
            lambda: ensure_file_for_date(date, config)
        )
        pickle_path = file_path.replace('.csv', '.pkl')

        if os.path.isfile(pickle_path):
            print("Cached pickled data found..")
            trajectories = pd.read_pickle(pickle_path)
        else:
            clean_sorted_ais = wrap_with_timings("Data Cleaning", lambda: clean_data(config, file_path))
            trajectories = wrap_with_timings("Trajectory construction", lambda: build_from_geopandas(clean_sorted_ais))
            trajectories.to_pickle(pickle_path)

        wrap_with_timings("Inserting trajectories", lambda: TrajectoryInserter().persist(trajectories, config))


if __name__ == '__main__':
    main(sys.argv[1:])
