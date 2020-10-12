package net.alexott.demos.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.{DatasetComparer, DatasetSchemaMismatch}
import org.scalatest.FreeSpec


// instantiate SparkSession
trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }
}

class TestSparkFastTests extends FreeSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  "should allow aliases a DataFrame" in {

    val sourceDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")
    ).toDF("name")

    val actualDF = sourceDF.select(col("name").alias("student"))

    val expectedDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")
    ).toDF("student")

    assertSmallDatasetEquality(actualDF, expectedDF)
  }

  "error when schemas don't match" in {
    val sourceDF = Seq(
      (1, "a"),
      (5, "b")
    ).toDF("number", "letter")

    val expectedDF = Seq(
      (1, "a"),
      (5, "b")
    ).toDF("num", "letter")

    assertThrows[DatasetSchemaMismatch] {
      assertSmallDatasetEquality(sourceDF, expectedDF)
    }
  }


}
