package net.alexott.fasttext_langdetect

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.DoubleParam

class FastTextLangDetectTransformer(override val uid: String)
  extends UnaryTransformer[String, String, FastTextLangDetectTransformer] {

  def this() = this(Identifiable.randomUID("FastTextLangDetectTF"))

  val minProbability = new DoubleParam(this, "binary",
    "Minimal probability for accepting the results of the language detection. Default: 0.7")
  setDefault(minProbability -> FastTextLangDetector.DEFAULT_MINIMUM_PROBABILITY)

  def setMinProbability(prob: Double): this.type = set(minProbability, prob).asInstanceOf[this.type ]

  protected override def createTransformFunc: String => String =
    (text: String) => FastTextLangDetector.detectLanguage(text,
      get(minProbability).getOrElse(FastTextLangDetector.DEFAULT_MINIMUM_PROBABILITY)).orNull

  override def outputDataType: DataType = StringType

  override def validateInputType(inputType: DataType): Unit = {
    require(isSet(inputCol), "input column must be set!")
    val inputColName = get(inputCol).get
    require(inputType == StringType,
      s"The input column $inputColName must be string type, but got $inputType.")
  }

  override def toString: String = {
    s"HashingTF: uid=$uid, compressed lang model? ${FastTextLangDetector.isCompressedLangModel}"
  }

}
