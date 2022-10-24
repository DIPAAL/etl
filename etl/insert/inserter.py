from etl.helper_functions import get_connection


def get_ship_id(dataframe, conn):
    """
    Using insert on conflict do nothing to get the id.
    imo int NOT NULL,
    mmsi int NOT NULL,
    mobile_type text NOT NULL,
    ship_type text,
    ship_name text,
    ship_callsign text,
    a float,
    b float,
    c float,
    d float
    :param row:
    :param conn:
    :return:
    """

    cursor = conn.cursor()

    $query = """
        INSERT INTO dim_ship (imo, mmsi, mobile_type, ship_type, ship_name, ship_callsign, a, b, c, d)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (imo, mmsi, mobile_type, ship_type, ship_name, ship_callsign, a, b, c, d) DO NOTHING
        RETURNING id, imo, mmsi, mobile_type, ship_type, ship_name, ship_callsign, a, b, c, d;
    """

    cursor.execute(query, (row['imo'], row['mmsi'], row['mobile_type'], row['ship_type'], row['ship_name'], row['ship_callsign'], row['a'], row['b'], row['c'], row['d']))
    result = cursor.fetchone()

    return result[0]


def ensure_ship_dimension(row, conn):

def insert_bulk(data_frame, config):
    # Group by the unique ship properties
    grouped = data_frame.groupby(['imo', 'mmsi', 'mobile_type', 'ship_type', 'ship_name', 'ship_callsign', 'a', 'b', 'c', 'd'])

    # Add ship_id to each row
