package net.alexott.fasttext_langdetect

import com.github.jfasttext.JFastText
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

case class LangDetectionResult(language: String, probability: Double)
case class LangDetectionResults(mainLanguage: String, allResults: Seq[LangDetectionResult])

/**
 *
 */
object FastTextLangDetector {
  private val modelName: String = "lid.176.ftz"

  val isCompressedLangModel: Boolean = (modelName == "lid.176.ftz")

  // TODO: use system property to load compressed or full language model...
  @transient
  lazy val langModel: Try[JFastText] = Try {
    val is = FastTextLangDetector.getClass.getResourceAsStream("/" + modelName)
    val jft = new JFastText(is)
    is.close()
    jft
  }

  private val LOGGER = LoggerFactory.getLogger(FastTextLangDetector.getClass)
  val UNKNOWN_LANGUAGE = "und"
  private val LABEL_PREFIX = "__label__"

  val DEFAULT_MINIMUM_PROBABILITY: Double = 0.7

  /**
   * Detects "primary" language for given text
   *
   * @param text text for language detection
   * @param minProb minimal probability
   * @return None if there are problems with loading of the model, or Some(language), with `und`
   *         if the probability is lower than `minProb`
   */
  def detectLanguage(text: String, minProb: Double = DEFAULT_MINIMUM_PROBABILITY): Option[String] = {
    langModel match {
      case Success(model) =>
        val res = model.predictProba(text)
        if (res != null && res.label.startsWith(LABEL_PREFIX) && res.logProb >= minProb)
          Some(res.label.substring(LABEL_PREFIX.length))
        else {
          LOGGER.info("Detection results: {}", res)
          Some(UNKNOWN_LANGUAGE)
        }
      case Failure(e) =>
        LOGGER.warn("Model is not initialized. Exception: {}", e.getMessage)
        None
    }
  }

  /**
   * Detects N languages for given text, selecting one with the highest probability
   * as a main language (if it's above `minProb`)
   *
   * @param text text for language detection
   * @param minProb minimal probability to promote one of the detected languages to the main
   * @param n how many candidates to include into results
   * @return instance of the `LangDetectionResults` with detected "main" language, and sequence
   *         of the detected languages sorted by probability in descending order
   */
  def detectLanguages(text: String, minProb: Double = DEFAULT_MINIMUM_PROBABILITY,
                      n: Int = 3): Option[LangDetectionResults] = {
    langModel match {
      case Success(model) =>
        val result = model.predictProba(text, n).asScala
          .filter(x => x.label.startsWith(LABEL_PREFIX))
          .sortBy(x => -x.logProb)
          .map(x => LangDetectionResult(x.label.substring(LABEL_PREFIX.length), x.logProb))
        if (result.isEmpty) {
          LOGGER.info("Detection results: {}", result)
          Some(LangDetectionResults(UNKNOWN_LANGUAGE, Seq()))
        } else if (result.head.probability >= minProb) {
          Some(LangDetectionResults(result.head.language, result))
        } else {
          Some(LangDetectionResults(UNKNOWN_LANGUAGE, result))
        }
      case Failure(e) =>
        LOGGER.warn("Model is not initialized. Exception: {}", e.getMessage)
        None
    }

  }

}
