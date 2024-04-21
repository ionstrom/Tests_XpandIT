package org.apache.spark

import org.apache.spark.sql.SparkSession

object Part2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Part2")
      .config("spark.master", "local")
      .getOrCreate()

    val dataApps = spark.read
      .option("header","true")
      .csv("C:/scala_testes/googleplaystore.csv")

    val bestApps = dataApps.filter(col("Rating") >=4.0)
    .orderBy(desc("Rating"))

    bestApps.write
      .option("header","true")
      .option("delimiter", "§")
      .csv("C:/scala_testes/best_apps.csv")

    spark.stop()
  }
}