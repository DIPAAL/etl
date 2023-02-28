"""Module for initializing the database."""
from etl.helper_functions import wrap_with_timings, get_connection
from etl.init.sqlrunner import run_sql_folder_with_timings, run_sql_file_with_timings, \
    run_single_statement_sql_files_in_folder


def setup_citus_instance(host, config):
    """
    Run commands on a single citus instance for setup.

    Args:
        host: the citus host to run the commands on
        config: the application configuration
    """
    conn = get_connection(config, host=host, database='postgres')
    conn.set_session(autocommit=True)

    if config['Database']['drop_database_on_init']:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {config['Database']['database']} WITH (FORCE);")

    run_sql_file_with_timings('etl/init/setup_database.sql', config, conn)

    conn = get_connection(config, host=host)
    conn.set_session(autocommit=True)
    run_sql_file_with_timings('etl/init/citus_ready.sql', config, conn)


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
        conn.cursor().execute(f"SELECT citus_add_node('{worker}', {port});")
    conn.commit()


def init_database(config):
    """
    Drop and recreate the database and all tables.

    Args:
        config: the application configuration
    """
    setup_citus_instances(config)
    setup_master(config)
    run_sql_folder_with_timings('etl/init/sql', config)
    run_single_statement_sql_files_in_folder('etl/init/single_statement_sql', config)
