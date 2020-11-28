package net.alexott.fasttext_langdetect

import com.github.jfasttext.JFastText
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object FastTextLangDetector {
  // TODO: use system property to load compressed or full language model...
  lazy val langModel: Try[JFastText] = Try {
    val is = FastTextLangDetector.getClass.getResourceAsStream("/lid.176.ftz")
    val jft = new JFastText(is)
    is.close()
    jft
  }

  private val LOGGER = LoggerFactory.getLogger(FastTextLangDetector.getClass)
  private val UNKNOWN_LANGUAGE = "unk"
  private val LABEL_PREFIX = "__label__"

  def detectLanguage(text: String): Option[String] = {
    langModel match {
      case Success(model) =>
        val label = model.predict(text)
        if (label != null && label.startsWith(LABEL_PREFIX))
          Some(label.substring(LABEL_PREFIX.length))
        else
          Some(UNKNOWN_LANGUAGE)
      case Failure(e) =>
        LOGGER.warn("Model is not initialized. Exception: {}", e.getMessage)
        None
    }
  }

}
