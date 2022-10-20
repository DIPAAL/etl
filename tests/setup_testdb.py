"""A collection of functions to help setting up and manipulating a PostgreSQL DB for testing purposes
"""
__author__ = "Mikael Vind Mikkelsen"
__maintainer__ = ""

import configparser
from etl.init_database import init_database


def initialize_testing_db():
    """Initialize the DB, destroy and recreate if already exist"""
    path = '.tests/config-testing.properties'
    config = configparser.ConfigParser()
    config.read(path)
    init_database(config)
    return config

