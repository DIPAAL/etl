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

global conn
def get_connection(config):
    global conn
    if conn is not None:
        return conn

    conn = psycopg2.connect(
        host=config['Database']['host'],
        database=config['Database']['database'],
        user=config['Database']['user'],
        password=config['Database']['password'],
        port=config['Database']['port']
    )
    return conn