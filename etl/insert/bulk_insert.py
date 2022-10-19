"""This module performs bulk insertion for the DWH
The module can take data frames and insert all rows from these into a specified table within a DWH"""

__author__ = "Mikael Vind Mikkelsen"
__maintainer__ = ""

import psycopg2
from io import StringIO
from etl.helper_functions import get_connection


def insert_bulk(data_frame, target_table, config):
    conn = get_connection(config)
    cursor = conn.cursor()

    """Creates a buffer, converts the data frame into a CSV file and store it in the buffer"""
    buffer = StringIO()
    data_frame.to_csv(buffer, index_label='id', header=False)
    buffer.seek(0)

    try:
        cursor.copy_from(buffer, target_table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    print("Insertion done")
    cursor.close()