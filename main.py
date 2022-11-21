"""The main module."""
import sys
import argparse
import configparser
import os
from datetime import datetime, timedelta

import pandas as pd

from etl.gatherer.file_downloader import ensure_file_for_date
from etl.helper_functions import wrap_with_timings
from etl.init_database import init_database
from etl.cleaning.clean_data import clean_data
from etl.insert.insert_trajectories import TrajectoryInserter
from etl.insert.insert_audit import AuditInserter
from etl.rollup.apply_rollups import apply_rollups
from etl.trajectory.builder import build_from_geopandas
from etl.constants import GLOBAL_AUDIT_LOGGER, ETL_PROJECT_VERSION

def get_config():
    """Get the application configuration."""
    path = './config.properties'
    local_path = './config-local.properties'
    if os.path.isfile(local_path):
        path = local_path
    config = configparser.ConfigParser()
    config.read(path)

    return config


def main(argv):
    """Execute the main program."""
    config = get_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--init", help="Initialize database", action="store_true")
    parser.add_argument("--load", help="Perform loading of the dates", action="store_true")
    parser.add_argument("--clean", help="Clean the given AIS data file", action="store_true")
    parser.add_argument("--from_date",
                        help="The date to load from, in the format YYYY-MM-DD, for example 2022-12-31", type=str)
    parser.add_argument("--to_date",
                        help="The date to load to, in the format YYYY-MM-DD, for example 2022-12-31", type=str)

    args = parser.parse_args()

    date_from = datetime.strptime(args.from_date, '%Y-%m-%d') if args.from_date else None
    date_to = datetime.strptime(args.to_date, '%Y-%m-%d') if args.to_date else None

    if args.init:
        wrap_with_timings("Database init", lambda: init_database(config), audit_log=False)

    if args.clean:
        clean_range(date_from, date_to, config)


def clean_range(date_from: datetime, date_to: datetime, config):
    """
    Apply cleaning for all dates in the given range.

    Date from must be before or equal to date to.

    Arguments:
        date_from: the date to start from
        date_to: the date to end at
    """
    # ensure date_from and date_to is set
    if not date_from or not date_to:
        raise ValueError("Please provide both from_date and to_date when cleaning data")

    # ensure date_from is before or equal to date_to
    if date_from > date_to:
        raise ValueError("from_date must be before or equal to to_date")

    # loop through all dates and clean them
    while date_from <= date_to:
        wrap_with_timings(f'Cleaning data for {date_from}', lambda: clean_date(date_from, config))
        date_from += timedelta(days=1)


def clean_date(date: datetime, config):
    """
    Apply cleaning, trajectory construction, and insert/rollup the results.

    Arguments:
        date: the date to clean
        config: the application configuration
    """
    file_path = wrap_with_timings(
        "Ensuring file for current date exists",
        lambda: ensure_file_for_date(date, config),
        audit_log=False
    )
    GLOBAL_AUDIT_LOGGER.log_file(file_path)  # logs the name, rows and size of the file
    pickle_path = file_path.replace('.csv', '.pkl')

    if os.path.isfile(pickle_path):
        print("Cached pickled data found..")
        trajectories = pd.read_pickle(pickle_path)
    else:
        clean_sorted_ais = wrap_with_timings("Data Cleaning", lambda: clean_data(config, file_path),
                                             audit_log=True, audit_name="cleaning")
        trajectories = wrap_with_timings("Trajectory construction", lambda: build_from_geopandas(clean_sorted_ais),
                                         audit_log=True, audit_name="trajectory")
        trajectories.to_pickle(pickle_path)

    conn = wrap_with_timings("Inserting trajectories", lambda: TrajectoryInserter().persist(trajectories, config),
                             audit_log=True, audit_name="bulk_insert")
    wrap_with_timings("Applying rollups", lambda: apply_rollups(conn, date),
                      audit_log=True, audit_name="cell_construct")

    GLOBAL_AUDIT_LOGGER.log_etl_version(ETL_PROJECT_VERSION) # logs the version of the ETL project
    GLOBAL_AUDIT_LOGGER.log_requirements() # logs the versions of the requirements
    wrap_with_timings("Inserting audit", lambda: AuditInserter().insert_audit(conn))
    GLOBAL_AUDIT_LOGGER.reset_log() # reset the log for the next loop

    conn.commit()

if __name__ == '__main__':
    main(sys.argv[1:])
