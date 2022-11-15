"""Module for initializing the database."""
from etl.helper_functions import wrap_with_timings, get_connection
from etl.init.sqlrunner import run_sql_folder_with_timings, run_sql_file_with_timings


def setup_citus_instance(host, config):
    """
    Run commands on a single citus instance for setup.

    Args:
        host: the citus host to run the commands on
        config: the application configuration
    """
    conn = get_connection(config, host=host, database='postgres')
    conn.set_session(autocommit=True)
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


def create_fact_partitions(config):
    """
    Create partitions for the fact tables.

    Args:
        config: the application configuration
    """
    conn = get_connection(config)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    # create monthly partitions for fact table for each dim_date
    cur.execute("SELECT DISTINCT year, month_of_year FROM dim_date ORDER BY year, month_of_year;")
    for date in cur.fetchall():
        year, month = date
        # 0 pad month to 2 digits
        month = str(month).zfill(2)

        if year is None or month is None:
            continue
        smart_key = int(f"{year}{month}00")
        # add 99 to the smart key, as the last two digits are reserved for the day
        cur.execute(f"""
            CREATE TABLE fact_trajectory_{year}_{month}
            PARTITION OF fact_trajectory FOR VALUES FROM ('{smart_key}') TO ('{smart_key + 99}');
        """)

        cur.execute(f"""
            CREATE TABLE fact_cell_{year}_{month}
            PARTITION OF fact_cell FOR VALUES FROM ('{smart_key}') TO ('{smart_key + 99}');
        """)


def init_database(config):
    """
    Drop and recreate the database and all tables.

    Args:
        config: the application configuration
    """
    setup_citus_instances(config)
    setup_master(config)
    run_sql_folder_with_timings('etl/init/sql', config)
    wrap_with_timings("Creating fact partitions", lambda: create_fact_partitions(config))
