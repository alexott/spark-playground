package net.alexott.fasttext_langdetect

import org.apache.spark.sql.SparkSession
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.flatspec.AnyFlatSpec


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

class FastTextLangDetectTransformerTest extends AnyFlatSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  val sourceDF = Seq(
    ("Это русский текст"),
    ("Xin chào"),
    ("Das ist Deutsch"),
    ("This is English"),
    ("q\"1k\"`2f-7=2+h`w`h`q`00`1`2")
  ).toDF("text")

  "lang detector" should "detect language" in {

    val langDetectTransformer = new FastTextLangDetectTransformer()
      .setInputCol("text")
      .setOutputCol("language")

    val actualDF = langDetectTransformer.transform(sourceDF)

    val expectedDF = Seq(
      ("Это русский текст", "ru"),
      ("Xin chào", "vi"),
      ("Das ist Deutsch", "de"),
      ("This is English", "en"),
      ("q\"1k\"`2f-7=2+h`w`h`q`00`1`2", FastTextLangDetector.UNKNOWN_LANGUAGE)
    ).toDF("text", "language")

    assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
  }

  it should "detect language with low probability" in {

    val langDetectTransformer = new FastTextLangDetectTransformer()
      .setInputCol("text")
      .setOutputCol("language")
      .setMinProbability(0.3)

    val actualDF = langDetectTransformer.transform(sourceDF)

    val expectedDF = Seq(
      ("Это русский текст", "ru"),
      ("Xin chào", "vi"),
      ("Das ist Deutsch", "de"),
      ("This is English", "en"),
      ("q\"1k\"`2f-7=2+h`w`h`q`00`1`2", "sah")
    ).toDF("text", "language")

    assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
  }


}
