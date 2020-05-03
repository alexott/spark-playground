package net.alexott.demos.spark

import org.apache.spark.SparkContext
import org.graphframes._
import org.apache.spark.sql.functions._

object GraphframeToZeppelin {
  // TODO: write function that will take vertex & edge labels from GraphFrame itself...


  def graphframeToNetworkText(g: GraphFrame, vertexLabel: String, edgeLabel: String): String = {
    val v = g.vertices
    val vDataFields = v.schema.fields.map{_.name}.filterNot(_ == "id").map(col(_))
    val vJson = v.select(col("id"), lit(vertexLabel).as("label"),
      struct(vDataFields: _*).as("data"))
      .toJSON.collect.mkString("[",", ","]")
    val e = g.edges
    val eDataFields = e.schema.fields.map{_.name}
      .filterNot(x => x == "src" || x == "dst").map(col(_))
    val timeUUID = udf(() => java.util.UUID.randomUUID().toString)
    val eJson = e.select(col("src").as("source"),
      col("dst").as("target"),
      lit(edgeLabel).as("label"),
      struct(eDataFields: _*).as("data"))
      .withColumn("id", timeUUID())
      .toJSON.collect.mkString("[",", ","]")

    val sb = new StringBuilder
    sb.append("%network {")
      .append("\n\"nodes\": ")
      .append(vJson)
      .append(",\n\"edges\": ")
      .append(eJson)
      .append(",\n\"directed\": false,\n\"types\":[\"")
      .append(edgeLabel)
      .append("\"],\n\"labels\":{\"")
      .append(vertexLabel)
      .append("\": \"#3071A9\"}\n}")

    sb.toString()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkContext.getOrCreate()

    val g: GraphFrame = examples.Graphs.friends
    print(graphframeToNetworkText(g, "person", "rel"))
    // g.edges.show()
    // g.vertices.show()

  }
}
