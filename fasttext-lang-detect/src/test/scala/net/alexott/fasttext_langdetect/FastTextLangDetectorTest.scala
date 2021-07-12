package net.alexott.fasttext_langdetect

import org.scalatest.flatspec.AnyFlatSpec

class FastTextLangDetectorTest extends AnyFlatSpec  {

  "lang detector (single)" should "detect russian language" in {
    val lang = FastTextLangDetector.detectLanguage("Это русский текст")
    assertResult(Some("ru"))(lang)
  }

  it should "detect vietnamese language" in {
    val lang = FastTextLangDetector.detectLanguage("Xin chào")
    assertResult(Some("vi"))(lang)
  }

  it should "detect german language" in {
    val lang = FastTextLangDetector.detectLanguage("Das ist Deutsch")
    assertResult(Some("de"))(lang)
  }

  it should "detect english language" in {
    val lang = FastTextLangDetector.detectLanguage("This is English")
    assertResult(Some("en"))(lang)
  }

  it should "not detect language" in {
    val lang = FastTextLangDetector.detectLanguage("q\"1k\"`2f-7=2+h`w`h`q`00`1`2")
    assertResult(Some(FastTextLangDetector.UNKNOWN_LANGUAGE))(lang)
  }

  it should "detect language with low probability" in {
    val lang = FastTextLangDetector.detectLanguage("q\"1k\"`2f-7=2+h`w`h`q`00`1`2", 0.3)
    assertResult(Some("sah"))(lang)
  }

  "lang detector (multiple)" should "detect main language" in {
    val lang = FastTextLangDetector.detectLanguages("Das ist Deutsch")
    //println(lang)
    assertResult("de")(lang.get.mainLanguage)
  }

  it should "detect mixed language" in {
    val lang = FastTextLangDetector.detectLanguages("Das ist Deutsch и русский тоже and Xin chào", n = 5)
    assertResult(FastTextLangDetector.UNKNOWN_LANGUAGE)(lang.get.mainLanguage)
  }

  it should "detect nothing" in {
    val lang = FastTextLangDetector.detectLanguages("")
    assertResult(Some(LangDetectionResults(FastTextLangDetector.UNKNOWN_LANGUAGE, Seq())))(lang)
  }


}
