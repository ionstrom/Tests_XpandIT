package org.apache.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Part5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Part5")
      .config("spark.master", "local")
      .getOrCreate()

    val dataApps = spark.read
      .option("header","true")
      .csv("C:/scala_testes/unique_App_Info.csv")

    val createMetrics = dfApps.withColumn("Genres", explode(split(col("Genres"), ";")))
      .groupBy("Genres")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    createMetrics.write
      .parquet("path/to/googleplaystore_metrics.parquet")

    spark.stop()
  }
}