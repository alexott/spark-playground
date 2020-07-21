package net.alexott.demos.spark

import org.apache.spark.sql.SparkSession

object SparkNLPLangDetectionTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    import com.johnsnowlabs.nlp.SparkNLP
    import org.apache.spark.sql.functions._
    import com.johnsnowlabs.nlp._
    import org.apache.spark.ml.Pipeline
    import com.johnsnowlabs.nlp.annotator._


    println(s"Using SparkNLP version ${SparkNLP.version()}")

    // languages supported by Spark NLP model
    val languages = Set("bg", "cz", "de", "el", "en", "es", "fi", "fr", "hr", "hu", "it",
      "no", "pl", "pt", "ro", "ru", "sl", "sv", "tr", "uk")

    val testData = spark.read.text("/Users/ott/development/NLP/lang-detect-data")
      .select($"value".as("text"),
        regexp_extract(input_file_name(), "^.*/([a-z]*)\\.txt$", 1).as("src_lang"))
      .where($"src_lang".isInCollection(languages))
      .cache()

    testData.groupBy("src_lang").count.show(5)

    // import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
    // val pipeline = PretrainedPipeline("detect_language_20", lang = "xx")
    // val annotation = pipeline.transform(testData)

    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val langDetector = LanguageDetectorDL.pretrained("ld_wiki_20")
      .setInputCols("document").setOutputCol("lang")
      .setThreshold(0.6f)
      .setCoalesceSentences(true)

    val languagePipeline = new Pipeline().setStages(Array(documentAssembler, langDetector))
    val results = languagePipeline.fit(testData).transform(testData)
      .select($"text", $"src_lang", $"lang".getItem(0).getItem("result").as("det_lang"))
      .cache()

    val countsByLanguage = testData.groupBy("src_lang").count
    val detectsByLanguage = results.select($"src_lang",
      when($"det_lang" === $"src_lang", 1).otherwise(0).as("correct"))
      .groupBy($"src_lang").agg(sum("correct").as("correct"))
    val finalResults = countsByLanguage.join(detectsByLanguage, countsByLanguage("src_lang") === detectsByLanguage("src_lang"))
      .select(countsByLanguage("src_lang"), $"count", $"correct", ($"correct"/$"count").as("precision"))

    finalResults.show(languages.size)

// looking for incorrect results
//    results.filter($"src_lang" === "es").filter($"det_lang" =!= "es").show(10)
//    for (lang <- languages) {
//      println(s"Looking for wrong results for language: $lang")
//      results.filter($"src_lang" === lang).filter($"det_lang" =!= lang)
//        .select($"src_lang", $"det_lang", length($"text").as("text_length"), $"text")
//        .show(200)
//    }

    results.filter($"det_lang" =!= $"src_lang")
      .select($"src_lang", $"det_lang", length($"text").as("text_length"), $"text")
      .sort($"src_lang".asc)
      .coalesce(1)
      .write // we can also do .partitionBy("$src_lang") to output by language, then disable coalesce
      .json("file:/tmp/wrong_detectsions.txt")

  }
}
