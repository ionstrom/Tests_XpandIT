package org.apache.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Part4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Part4")
      .config("spark.master", "local")
      .getOrCreate()

    val dataSentiment = spark.read
      .option("header","true")
      .csv("path/to/sentiment_analysis.csv")


    val dataApps = spark.read
      .option("header","true")
      .csv("C:/scala_testes/unique_App_Info.csv")

    val finalData = dataApps.join(dataSentiment, Seq("App"), "inner")

    finalData.write
      .parquet("C:/scala_testes/googleplaystore_cleaned.parquet")

    spark.stop()
  }
}
