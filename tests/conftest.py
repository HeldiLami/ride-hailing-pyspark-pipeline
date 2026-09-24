import os
import sys

import pytest

from src.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = get_spark_session("ride-hailing-tests")
    yield session
    session.stop()
