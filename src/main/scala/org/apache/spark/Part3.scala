package org.apache.spark

import org.apache.spark.sql.SparkSession

object Part3{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Part3")
      .config("spark.master", "local")
      .getOrCreate()


    val dataApps = spark.read
      .option("header","true")
      .csv("C:/scala_testes/googleplaystore.csv")

    val uniqueAppInfo = dataApps.groupBy("App")
      .agg(
        collect_list("Category").alias("Categories"),
        max("Rating").alias("Rating"),
        max("Reviews").alias("Reviews"),
        max("Size").alias("Size"),
        max("Installs").alias("Installs"),
        max("Type").alias("Type"),
        max("Price").alias("Price"),
        max("Content Rating").alias("Content_Rating"),
        collect_list("Genres").alias("Genres"),
        max("Last Updated").alias("Last_Updated"),
        max("Current Ver").alias("Current_Version"),
        max("Android Ver").alias("Minimum_Android_Version")
      )

    uniqueAppsDF.write
      .option("header", "true")
      .csv("C:/scala_testes/unique_App_Info.csv")

    spark.stop()

  }
}