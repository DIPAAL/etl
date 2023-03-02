"""The main module."""
import sys
import argparse
import configparser
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Generator, Tuple

from etl.benchmark_runner.benchmark_runner import BenchmarkRunner
from etl.gatherer.file_downloader import ensure_file_for_date
from etl.helper_functions import wrap_with_timings, extract_date_from_smart_date_id
from etl.init_database import init_database
from etl.cleaning.clean_data import clean_data
from etl.insert.insert_trajectories import TrajectoryInserter
from etl.insert.insert_audit import AuditInserter
from etl.rollup.apply_rollups import apply_rollups
from etl.trajectory.builder import build_from_geopandas
from etl.audit.logger import global_audit_logger as gal, ROWS_KEY
from etl.constants import ETL_STAGE_CLEAN, ETL_STAGE_TRAJECTORY, ETL_STAGE_BULK, ETL_STAGE_CELL, T_START_DATE_COL


def get_config():
    """Get the application configuration."""
    path = './config.properties'
    local_path = './config-local.properties'
    if os.path.isfile(local_path):
        path = local_path
    config = configparser.ConfigParser()
    config.read(path)

    return config


def configure_arguments():
    """Configure the program argument parser."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--init', help='Initialize data warehouse and setup cluster', action='store_true')
    parser.add_argument('--load', help="Load AIS data from given dates", action="store_true")
    parser.add_argument('--clean_standalone',
                        help='Standalone clean AIS data and construct trajectories which are stored as .pkl files',
                        action='store_true')
    parser.add_argument('--querybenchmark', help='Perform query benchmark', action='store_true')
    parser.add_argument('--ensure_files', help='Runs the file downloader for a given date range.', action='store_true')
    parser.add_argument('--from_date',
                        help='The date to load from, in the format YYYY-MM-DD, for example 2022-12-31', type=str)
    parser.add_argument('--to_date',
                        help='The date to load to, in the format YYYY-MM-DD, for example 2022-12-31', type=str)

    return parser.parse_args()


def main(argv):  # noqa: C901
    """Execute the main program."""
    config = get_config()

    args = configure_arguments()

    date_from = datetime.strptime(args.from_date, '%Y-%m-%d') if args.from_date else None
    date_to = datetime.strptime(args.to_date, '%Y-%m-%d') if args.to_date else None

    if args.init:
        wrap_with_timings("Database init", lambda: init_database(config))

    if args.clean_standalone or args.load:
        ais_gen = clean_range(date_from, date_to, config, args.clean_standalone)
        for _, ais_data in ais_gen:
            load_data(ais_data, config) if args.load else None

    if args.querybenchmark:
        BenchmarkRunner(config).run_benchmark()

    if args.ensure_files:
        ensure_files_for_range(date_from, date_to, config)


def clean_range(date_from: datetime, date_to: datetime, config, standalone: bool = False) -> \
        Generator[Tuple[datetime, pd.DataFrame], None, None]:
    """
    Load data for all dates in the given range.

    Date from must be before or equal to date to.

    Arguments:
        date_from: the date to start from
        date_to: the date to end at
        standalone: whether standalone cleaning is run (default: False)
    """
    # ensure date_from and date_to is set
    if not date_from or not date_to:
        raise ValueError('Please provide both "from_date" and "to_date" when loading data')

    # ensure date_from is before or equal to date_to
    if date_from > date_to:
        raise ValueError('"from_date" must be before or equal to "to_date"')

    # loop through all dates and clean them
    while date_from <= date_to:
        yield (date_from, wrap_with_timings(
                            f'Cleaning data for {date_from}',
                            lambda: clean_date(date_from, config, standalone)
                        ))
        date_from += timedelta(days=1)


def clean_date(date: datetime, config, standalone: bool = False) -> pd.DataFrame:
    """
    Apply cleaning and trajectory construction.

    Arguments:
        date: the date to clean
        config: the application configuration
        standalone: whether standalone cleaning is run (Default: False)
    """
    file_path = wrap_with_timings(
        "Ensuring file for current date exists",
        lambda: ensure_file_for_date(date, config),
    )

    gal.log_file(file_path)  # logs the name, rows and size of the file

    if file_path.endswith('.pkl'):
        print(f'Pickle file found for date {date}')
        return wrap_with_timings('Reading Pre-processed AIS Pickle', lambda: pd.read_pickle(file_path))

    clean_sorted_ais = wrap_with_timings('Data Cleaning', lambda: clean_data(config, file_path),
                                         audit_etl_stage=ETL_STAGE_CLEAN)
    gal[ROWS_KEY]['points_after_clean'] = len(clean_sorted_ais.index)
    trajectories = wrap_with_timings('Trajectory Construction', lambda: build_from_geopandas(clean_sorted_ais),
                                     audit_etl_stage=ETL_STAGE_TRAJECTORY)
    gal[ROWS_KEY]['trajectories_built'] = len(trajectories.index)

    if standalone:
        pickle_path = file_path.replace('.csv', '.pkl')
        wrap_with_timings('Pickle Creation', lambda: trajectories.to_pickle(pickle_path))

    return trajectories


def load_data(data: pd.DataFrame, config) -> None:
    """
    Insert and rollup the data into the DW.

    Arguments:
        data: the dataframe containing the data
        config: the application config
    """
    # Extract the date to rollup from the dataframe
    smart_date_key = data[T_START_DATE_COL].iat[0]
    date = extract_date_from_smart_date_id(smart_date_key)
    conn = wrap_with_timings("Inserting trajectories",
                             lambda: TrajectoryInserter("fact_trajectory").persist(data, config),
                             audit_etl_stage=ETL_STAGE_BULK)
    wrap_with_timings("Applying rollups", lambda: apply_rollups(conn, date),
                      audit_etl_stage=ETL_STAGE_CELL)

    wrap_with_timings("Inserting audit", lambda: AuditInserter("audit_log").insert_audit(conn))
    gal.reset_log()  # reset the log for the next loop

    conn.commit()


def ensure_files_for_range(date_from: datetime, date_to: datetime, config):
    """
    Ensure files are downloaded and unzipped for a given date range.

    Keyword arguments:
        date_from: the date to start from
        date_to: the date to end at
        config: the application configuration
    """
    while date_from <= date_to:
        wrap_with_timings(f"Ensuring files for date {date_from}", lambda: ensure_file_for_date(date_from, config))
        date_from += timedelta(days=1)


if __name__ == '__main__':
    main(sys.argv[1:])
