This directory contains a simple implementation of the language detector based on the [FastText models for language detection](https://fasttext.cc/blog/2017/10/02/blog-post.html).  These models produce very good language detection results, even for the short texts and texts with mixed languages (see [my blog post from 3 years ago](https://alexott.blogspot.com/2017/10/evaluating-fasttexts-models-for.html)).  Current implementation uses "compressed" model that has slightly worse results than full model, but much smaller in size (less than 1Mb vs ~130Mb). 

Warning: when using these models, make sure that you're running it against "clean" text, because things like, emojis, URLs, Twitter user names, etc. heavily affecting the detection. 

# Build

This implementation depends on the [JFastText library](https://github.com/carschno/JFastText) that uses platform-specific code to use FastText models, and currently only Linux implementation is shipped to the Maven Central.

As result, on Linux, just do the `mvn package`...

On Mac or Windows you first need to compile and install JFastText - [fetch sources as described](https://github.com/carschno/JFastText#windows-and-mac-os-x) and do the `mvn install`, and only after that you can do `mvn package`.

The language detection model is embedded into the uberjar produced at `target/fasttext-lang-detect-0.0.1-jar-with-dependencies.jar`.

By default the implementation uses Spark 3.0.1 with Scala 2.12, but it should be possible to compile the code for the earlier Spark versions as well.

# Usage

Use the compiled uberjar in your project.

## Scala

There are 2 functions available right now:

* `net.alexott.fasttext_langdetect.FastTextLangDetector.detectLanguage` - detects the "main language" if its detection probability is higher than the given threshold (0.7 by default)
* `net.alexott.fasttext_langdetect.FastTextLangDetector.detectLanguages` - detects the "main language" as well, but also returns results (language and detection probability) for N languages


## Spark ML (Scala)

Start Spark pointing to the generated uberjar, like this:

```
bin/spark-shell --jars <path-to>/fasttext-lang-detect-0.0.1-jar-with-dependencies.jar
```

And in the code just instantiate the `FastTextLangDetectTransformer` class, and set input & output columns, like this: 

```scala
    val langDetectTransformer = new net.alexott.fasttext_langdetect.FastTextLangDetectTransformer()
      .setInputCol("text")
      .setOutputCol("language")

    val actualDF = langDetectTransformer.transform(sourceDF)
```

By default `FastTextLangDetectTransformer` uses 0.7 as minimum probability for language detection.  It could be explicitly set using the `setMinProbability` method:

```scala
    val langDetectTransformer = new net.alexott.fasttext_langdetect.FastTextLangDetectTransformer()
      .setInputCol("text")
      .setOutputCol("language")
      .setMinProbability(0.3)
```

## Spark ML (Python)

Coming soon...


# TODOs

* Allow to select full or the compressed model during compilation
* Python bindings
* Out of box registration of UDF for Spark SQL
