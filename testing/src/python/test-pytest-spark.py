import pyspark
import pyspark.sql
import pytest
from pyspark.sql import SparkSession


def test_spark_session_sql(spark_session):
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")
    test_df.createOrReplaceTempView("test")

    test_filtered_df = spark_session.sql("SELECT a, b from test where a > 1")
    assert test_filtered_df.count() == 1


def test_spark_context_fixture(spark_context):
    test_rdd = spark_context.parallelize([1, 2, 3, 4])
    assert test_rdd.count() == 4
