package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, col}

object Part1{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("P1")
      .config("spark.master", "local")
      .getOrCreate()

    val dataReveiws = spark.read
      .option("header", "true").schema("App STRING, Translated_Review STRING, Sentiment STRING, Sentiment_Polarity DOUBLE, Sentiment_Subjectivity DOUBLE")
      .csv("C:/scala_testes/googleplaystore_user_reviews")

    dataReveiws.show()

    val dataSentiment = dataReveiws.groupBy("App")
      .agg(avg(col("Sentiment_polarity")).alias("Average_Sentiment_Polarity"))
      .na.fill(0.0, Seq("Average_Sentiment_Polarity")

    dataSentiment.show()

    dataSentiment.write()
      .option("header","true")
      .csv("C:/scala_testes/sentiment_analysis.csv")

    spark.stop()
  }
}