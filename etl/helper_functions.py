from datetime import datetime, timedelta
from time import perf_counter
import psycopg2



def wrap_with_timings(name: str, func) -> None:
    print(f"{name} started at {datetime.now()}")
    start = perf_counter()
    func()
    end = perf_counter()
    print(f"{name} finished at {datetime.now()}")
    print(f"{name} took {timedelta(seconds=(end - start))}")

def get_connection(config, database=None, host=None, user=None, password=None):

    host, port = host.split(':') if host is not None else config['Database']['host'].split(':')
    database = database if database is not None else config['Database']['database']
    user = user if user is not None else config['Database']['user']
    password = password if password is not None else config['Database']['password']
    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )