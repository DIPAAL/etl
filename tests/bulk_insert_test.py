"""Explanation
"""
__author__ = "Mikael Vind Mikkelsen"
__maintainer__ = ""

from tests.setup_testdb import initialize_testing_db
from etl.insert.bulk_insert import insert_bulk


def test_insert_no_mobilitydb():
    config = initialize_testing_db()
