import psycopg2

from etl.helper_functions import wrap_with_timings, get_connection
from etl.init.sqlrunner import run_sql_folder_with_timings, run_sql_file_with_timings


def setup_citus_instance(host, config):
    host, port = host.split(':')
    conn = psycopg2.connect(
        host=host,
        database=config['Database']['database'],
        user=config['Database']['user'],
        password=config['Database']['password'],
        port=port
    )
    run_sql_file_with_timings('etl/init/citus_ready.sql', config, conn)

def setup_citus_instances(config):
    hosts = config['Database']['worker_connection_hosts'].split(',')
    hosts.append(f"config['Database']['host']:config['Database']['port']")
    for host in hosts:
        wrap_with_timings(f"Setup worker {host}", lambda: setup_citus_instance(host, config))



def setup_master(config):
    conn = get_connection(config)
    workers = config['Database']['worker_connection_hosts'].split(',')
    for worker in workers:
        worker, port = worker.split(':')
        conn.cursor().execute(f"SELECT citus_add_node('{worker}', {port});")
    conn.commit()


def init_database(config):
    setup_citus_instances(config)
    setup_master(config)
    run_sql_folder_with_timings('etl/init/sql', config)
