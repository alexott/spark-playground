package net.alexott.fasttext_langdetect

import org.scalatest.flatspec.AnyFlatSpec

class FastTextLangDetectorTest extends AnyFlatSpec  {

  "lang detector" should "detect russian language" in {
    val lang = FastTextLangDetector.detectLanguage("Это русский текст")
    assertResult(Some("ru"))(lang)
  }

  "lang detector" should "detect vietnamese language" in {
    val lang = FastTextLangDetector.detectLanguage("Xin chào")
    assertResult(Some("vi"))(lang)
  }

  "lang detector" should "detect german language" in {
    val lang = FastTextLangDetector.detectLanguage("Das ist Deutsch")
    assertResult(Some("de"))(lang)
  }

  "lang detector" should "detect english language" in {
    val lang = FastTextLangDetector.detectLanguage("This is English")
    assertResult(Some("en"))(lang)
  }
}
