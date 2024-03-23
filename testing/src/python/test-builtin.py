from pyspark.testing import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("builtin").getOrCreate()


def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")


def test_removes_non_word_characters_short():
    data = [("jo&&se", "jose"), ("**li**", "li"), ("#::luisa", "luisa"), (None, None)]
    df = spark.createDataFrame(data, ["name", "expected_name"]).withColumn(
        "clean_name", remove_non_word_characters(F.col("name"))
    )
    assertDataFrameEqual(df.selectExpr("clean_name as data"), df.selectExpr("expected_name as data"))


def test_remove_non_word_characters_nice_error():
    data = [("matt7", "matt"), ("bill&", "bill"), ("isabela*", "isabela"), (None, None)]
    df = spark.createDataFrame(data, ["name", "expected_name"]).withColumn(
        "clean_name", remove_non_word_characters(F.col("name"))
    )
    assertDataFrameEqual(df.selectExpr("clean_name as data"), df.selectExpr("expected_name as data"))
