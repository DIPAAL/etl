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

def get_connection(config):
    host, port = config["Database"]["host"].split(":")
    conn = psycopg2.connect(
        host=host,
        database=config['Database']['database'],
        user=config['Database']['user'],
        password=config['Database']['password'],
        port=port
    )
    return conn