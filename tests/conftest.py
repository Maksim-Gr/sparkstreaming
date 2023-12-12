import pytest
from pyspark.sql import SparkSession

APP_NAME = "aip_unit_testing"


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName(APP_NAME).getOrCreate()
