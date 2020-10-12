package net.alexott.demos.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException

case class MyData(name: String, price: Int)

class TestSparkTestingBase extends FunSuite with DataFrameSuiteBase {

  val inputList = List(
    MyData("test1", 123),
    MyData("test2", 42))


  test("dataframe should be equal with different order of rows") {
    import spark.implicits._
    val inputListWithDuplicates = inputList ++ List(inputList.head)
    val input = inputListWithDuplicates.toDF
    val reverseInput = inputListWithDuplicates.reverse.toDF

    assertDataFrameNoOrderEquals(input, reverseInput)
    assertDataFrameEquals(input, reverseInput)
  }

  test("empty dataframes should be not be equal to nonempty ones") {
    import spark.implicits._
    val emptyList = spark.emptyDataset[MyData].toDF()
    val input = inputList.toDF
    assertThrows[TestFailedException] {
      assertDataFrameEquals(emptyList, input)
    }
    assertThrows[TestFailedException] {
      assertDataFrameNoOrderEquals(emptyList, input)
    }
  }

}