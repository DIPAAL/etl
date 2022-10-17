"""This module performs bulk insertion for the DWH
The module can take data frames and insert all rows from these into a specified table within a DWH"""

__author__ = "Mikael Vind Mikkelsen"
__maintainer__ = ""

import psycopg2
import os
import pandas as pd
from io import StringIO
from etl.helper_functions import get_connection


def insert_trajectories(data_frame, target_table, config):
    conn = get_connection(config)
    cursor = conn.cursor()

    """Buffering the data frame as a CSV file
    NOTE - PERFORMANCE: Very memory intensive, could cause issues"""
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


def insert_iterator_trajectories(data_frame, table, config):
    conn = get_connection(config)
    cursor = conn.cursor()


def insert_cells(config):
    """TODO: Created function, once work on trajectory rollup module commences"""
    conn = get_connection(config)
    return 0


def _dataframe_buffer(data_frame):
    """Saves dataframe to an in memory buffer"""

    buffer = StringIO()
    data_frame.to_csv(buffer, index_label='id', header=False)
    return buffer.seek(0)