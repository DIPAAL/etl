"""Module for initializing the database."""
from etl.helper_functions import wrap_with_timings, get_connection, get_staging_cell_sizes
from etl.init.sqlrunner import run_sql_folder_with_timings, run_sql_file_with_timings, \
    run_single_statement_sql_files_in_folder
from etl.constants import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import text


def setup_citus_instance(host, config):
    """
    Run commands on a single citus instance for setup.

    Args:
        host: the citus host to run the commands on
        config: the application configuration
    """
    conn = get_connection(config, auto_commit_connection=True, host=host, database='postgres')

    if config['Database']['drop_database_on_init']:
        conn.execute(text(f"DROP DATABASE IF EXISTS {config['Database']['database']} WITH (FORCE);"))

    run_sql_file_with_timings('etl/init/setup_database.sql', config, conn, set_autocommit=False)

    conn = get_connection(config, auto_commit_connection=True, host=host)
    run_sql_file_with_timings('etl/init/citus_ready.sql', config, conn, set_autocommit=False)


def setup_citus_instances(config):
    """
    Run commands that need to be run on all citus instances for setup.

    Args:
        config: the application configuration
    """
    hosts = config['Database']['worker_connection_hosts'].split(',')
    hosts.append(config['Database']['host'])
    for host in hosts:
        wrap_with_timings(f"Setup worker {host}", lambda: setup_citus_instance(host, config))


def setup_master(config):
    """
    Run commands that need to be run only on master for setup.

    Args:
        config: the application configuration
    """
    conn = get_connection(config)
    workers = config['Database']['worker_connection_internal_hosts'].split(',')
    for worker in workers:
        worker, port = worker.split(':')
        conn.execute(text(f"SELECT citus_add_node('{worker}', {port});"))
    conn.commit()


def setup_staging_area(config):
    """
    Run commands to setup cell hierarchy.

    Args:
        config: the application configuration
    """
    STAGING_CELL_SIZES = get_staging_cell_sizes()
    for cell_size in STAGING_CELL_SIZES:
        run_sql_file_with_timings('etl/init/sql/staging/01_staging_cells.sql', config, format=dict(CELL_SIZE=cell_size))
    run_sql_file_with_timings('etl/init/sql/staging/02_staging_trajectory.sql', config)
    run_sql_file_with_timings('etl/init/sql/staging/03_staging_day_mapping.sql', config)
    run_sql_file_with_timings('etl/init/sql/staging/04_staging_month_mapping.sql', config)
    run_sql_file_with_timings('etl/init/sql/staging/05_staging_fixed_holidays.sql', config)
    run_sql_file_with_timings('etl/init/sql/staging/06_staging_mid_map.sql', config)


def init_database(config):
    """
    Drop and recreate the database and all tables.

    Args:
        config: the application configuration
    """
    setup_citus_instances(config)
    setup_master(config)
    run_sql_folder_with_timings('etl/init/sql', config)
    setup_staging_area(config)
    run_single_statement_sql_files_in_folder('etl/init/single_statement_sql', config)
