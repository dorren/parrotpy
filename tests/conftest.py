import pytest
import logging

from helpers.spark_helpers import *
from helpers.test_helpers import *


def pytest_configure(config):
    # Set default log level for a specific logger
    # logging.getLogger("my_module").setLevel(logging.DEBUG)
    # Set a general default level if needed
    logging.basicConfig(level=logging.INFO)