import pyspark
import pyspark.sql
import pytest
import shutil
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

delta_dir_name = "/tmp/delta-table"

@pytest.fixture
def delta_setup(spark_session):
    data = spark_session.range(0, 5)
    data.write.format("delta").save(delta_dir_name)
    yield data
    shutil.rmtree(delta_dir_name, ignore_errors=True)

def test_delta(spark_session, delta_setup):
    deltaTable = DeltaTable.forPath(spark_session, delta_dir_name)
    hist = deltaTable.history()
    assert hist.count() == 1

