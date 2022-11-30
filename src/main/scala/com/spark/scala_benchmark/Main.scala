package com.spark.scala_benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import java.sql.Date
import scala.collection.mutable


object Main extends App {

  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
  val CSV_DELIMITER = ","
  val CSV_PATH = "/home/nizam/opensky/"
  val PARQUET_PATH = "/home/nizam/opensky/opensky.parquet"
  val TEST_REPEAT_COUNT = 10
  val TEST_TYPES_COUNT = 5
  val WINDOW_SIZE_IN_DAYS = 30
  val DOUBLE_PRECISION = 5
  val EARTH_RADIUS = 6371000
  val DEG2RAD = 180 / Math.PI

  /**
   * preprocessParquetDataset - read dataset from CSV files, duplicate data with time offset (3 and 6 years)
   * (to increase the amount of data), save dataset to parquet files
   *
   * @param spark      - current spark session
   * @param inputPath  - path to CSV files directory (input data)
   * @param outputPath - path to parquet files directory (output data)
   */
  def preprocessParquetDataset(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    val optionsMap = mutable.HashMap("delimiter" -> CSV_DELIMITER, "header" -> "true", "inferSchema" -> "true")
    val opensky = spark.read.options(optionsMap).csv(inputPath)
      .withColumn("firstseen", to_date(col("firstseen"), TIMESTAMP_FORMAT))
      .withColumn("lastseen", to_timestamp(col("lastseen"), TIMESTAMP_FORMAT))
    val opensky2 = opensky
      .withColumn("firstseen", col("firstseen").minus(expr("INTERVAL 3 YEARS")))
      .withColumn("lastseen", col("lastseen").minus(expr("INTERVAL 3 YEARS")));
    val opensky3 = opensky
      .withColumn("firstseen", col("firstseen").minus(expr("INTERVAL 6 YEARS")))
      .withColumn("lastseen", col("lastseen").minus(expr("INTERVAL 6 YEARS")));
    val opensky4 = opensky.union(opensky2).union(opensky3)
      .withColumn("firstseen", date_format(col("firstseen"), TIMESTAMP_FORMAT))
      .withColumn("lastseen", date_format(col("lastseen"), TIMESTAMP_FORMAT));
    opensky4.write.format("parquet").save(outputPath)
  }


  /**
   * readParquetDataset - read dataset from parquet files,
   * selects columns (origin, destination, firstseen, latitude_1, longitude_1, latitude_2, longitude_2),
   * filters rows with nulls
   * converts firstseen, lastseen columns to timestamp type
   * converts latitude_1, longitude_1, latitude_2, longitude_2 columns to double type,
   *
   * @param spark - current spark session
   * @param path  - path to parquet files directory (input data)
   * @return dataframe with selected rows
   */
  def readParquetDataset(spark: SparkSession, path: String): DataFrame = {
    val optionsMap = mutable.HashMap("recursiveFileLookup" -> "true")
    spark.read.options(optionsMap).parquet(path)
      .select(col("origin"), col("destination"),
        col("firstseen"), col("lastseen"),
        col("latitude_1"), col("longitude_1"),
        col("latitude_2"), col("longitude_2"))
      .filter(col("origin").isNotNull)
      .filter(col("destination").isNotNull)
      .filter(col("latitude_1").isNotNull)
      .filter(col("longitude_1").isNotNull)
      .filter(col("latitude_2").isNotNull)
      .filter(col("longitude_2").isNotNull)
      .withColumn("firstseen", to_timestamp(col("firstseen"), TIMESTAMP_FORMAT))
      .withColumn("lastseen", to_timestamp(col("lastseen"), TIMESTAMP_FORMAT))
      .withColumn("latitude_1", col("latitude_1").cast(DataTypes.DoubleType))
      .withColumn("longitude_1", col("longitude_1").cast(DataTypes.DoubleType))
      .withColumn("latitude_2", col("latitude_2").cast(DataTypes.DoubleType))
      .withColumn("longitude_2", col("longitude_2").cast(DataTypes.DoubleType))
  }

  val spark = SparkSession.builder()
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .appName("SparkScalaTest")
    .master("local[8]")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  var TestTimes = Array.fill[Long](TEST_TYPES_COUNT)(0)
  var start = System.nanoTime
  val opensky = readParquetDataset(spark, PARQUET_PATH).persist
  var cnt = opensky.count
  var finish = System.nanoTime
  var timeElapsed = (finish - start) / 1e6.toLong
  println("Test reading dataset")
  println(s"Elapsed time ${timeElapsed} ms, selected ${cnt} records")
  println("Dataset schema")
  opensky.printSchema()
  //  print(spark.conf.get("spark.submit.deployMode"))
  for (i <- 0 until TEST_REPEAT_COUNT) {
    println(s"\n${i+1} attempt")

    spark.catalog.clearCache
    println("\t1-st query (reading with filtering) started...")
    val start1 = System.nanoTime
    val df1 = readParquetDataset(spark, PARQUET_PATH)
      .filter(col("origin").isin("UUEE", "UUDD", "UUWW"))
      .filter(to_date(col("firstseen"), "YYYY-MM-DD").geq(Date.valueOf("2020-06-01")))
      .filter(to_date(col("firstseen"), "YYYY-MM-DD").leq(Date.valueOf("2020-08-31"))).persist()
    val cnt1 = df1.count
    val finish1 = System.nanoTime
    val timeElapsed1 = (finish1 - start1) / 1e6.toLong
    println(s"\tElapsed time ${timeElapsed1} ms, selected ${cnt1} records")
    TestTimes.update(0, (TestTimes(0) + timeElapsed1))

    spark.catalog.clearCache
    println("\t2-st query (reading with aggregation) started...")
    val start2 = System.nanoTime
    val df2 = readParquetDataset(spark, PARQUET_PATH)
      .withColumn("flighttime", col("lastseen")
        .cast(DataTypes.LongType).minus(col("firstseen").cast(DataTypes.LongType)))
      .groupBy(col("origin"), col("destination"))
      .agg(avg("flighttime").alias("AvgFlightTime"))
      .orderBy(col("AvgFlightTime").desc).persist()
    val cnt2 = df2.count
    val finish2 = System.nanoTime
    val timeElapsed2 = (finish2 - start2) / 1e6.toLong
    println(s"\tElapsed time ${timeElapsed2} ms, selected ${cnt2} records")
    TestTimes.update(1, (TestTimes(1) + timeElapsed2))

    spark.catalog.clearCache
    println("\t3-st query (reading with aggregation and then filtering) started...")
    val ws3 = Window.orderBy(monotonically_increasing_id()).rowsBetween(Long.MinValue, Long.MaxValue)
    val start3 = System.nanoTime
    val df3 = readParquetDataset(spark, PARQUET_PATH)
      .withColumn("flighttime", col("lastseen").cast(DataTypes.LongType)
        .minus(col("firstseen").cast(DataTypes.LongType)))
      .groupBy(col("origin"), col("destination"))
      .agg(round(avg("flighttime"), DOUBLE_PRECISION).alias("AvgFlightTime"))
      .withColumn("TotalAvgFlightTime", round(
        avg("AvgFlightTime").over(ws3).cast(DataTypes.DoubleType), DOUBLE_PRECISION))
      .filter(col("AvgFlightTime").geq(col("TotalAvgFlightTime")))
      .orderBy(col("AvgFlightTime").desc).persist()
    val cnt3 = df3.count
    val finish3 = System.nanoTime
    val timeElapsed3 = (finish3 - start3) / 1e6.toLong
    println(s"\tElapsed time ${timeElapsed3} ms, selected ${cnt3} records")
    TestTimes.update(2, (TestTimes(2) + timeElapsed3))

    spark.catalog.clearCache
    println("\t4-st query (reading and calculating the maximum of a complex function in a sliding window) started...")
    val ws4 = Window.orderBy(col("flightdate"))
      .rowsBetween(Window.currentRow - WINDOW_SIZE_IN_DAYS, Window.currentRow)

    val start4 = System.nanoTime


    var df4 = readParquetDataset(spark, PARQUET_PATH)
      .withColumn("distance_km", round(lit(2 * EARTH_RADIUS).multiply(
        asin(sqrt(pow(sin(col("latitude_2").divide(lit(DEG2RAD)).minus(col("latitude_1")
          .divide(lit(DEG2RAD))).divide(lit(2.0))), 2.0)
          .plus(cos(col("latitude_1").divide(lit(DEG2RAD))).multiply(cos(col("latitude_2")
            .divide(lit(DEG2RAD)))).multiply(pow(sin(col("longitude_2").divide(lit(DEG2RAD))
            .minus(col("longitude_1").divide(lit(DEG2RAD))).divide(lit(2.0))), 2.0)))))), 0)
        .divide(lit(1000.0)))
      .withColumn("flightdate", to_date(col("firstseen")))
      .groupBy(col("flightdate"))
      .agg(max("distance_km").as("max_distance_km"))
      .select(
        col("flightdate"),
        max(col("max_distance_km")).over(ws4).as("max_distance_km_on_date")
      )
      .orderBy(col("flightdate").asc).persist()
    val cnt4 = df4.count
    val finish4 = System.nanoTime
    val timeElapsed4 = (finish4 - start4) / 1e6.toLong
    println(s"\tElapsed time ${timeElapsed4} ms, selected ${cnt4} records")
    TestTimes.update(3, (TestTimes(3) + timeElapsed4))


    spark.catalog.clearCache
    println("\t5-st query (reading and calculating the maximum of a complex udf in a sliding window) started...")
    val start5 = System.nanoTime
    val get_distance = (latitude_1: Double, longitude_1: Double, latitude_2: Double, longitude_2: Double) =>
      Math.round(2 * EARTH_RADIUS * Math.asin(Math.sqrt(
        Math.pow(Math.sin((Math.toRadians(latitude_2) - Math.toRadians(latitude_1)) / 2.0), 2.0)
          + Math.cos(Math.toRadians(latitude_1)) * Math.cos(Math.toRadians(latitude_2))
          * Math.pow(Math.sin((Math.toRadians(longitude_2) - Math.toRadians(longitude_1)) / 2.0), 2.0)))) / 1000.0
    spark.udf.register("get_distance", get_distance)
    var df5 = readParquetDataset(spark, PARQUET_PATH)
      .withColumn("distance_km",
        callUDF("get_distance", col("latitude_1"), col("longitude_1"),
          col("latitude_2"), col("longitude_2")))
      .withColumn("flightdate", to_date(col("firstseen")))
      .groupBy(col("flightdate"))
      .agg(max("distance_km").as("max_distance_km"))
      .select(col("flightdate"), max(col("max_distance_km")).over(ws4).as("max_distance_km_on_date"))
      .orderBy(col("flightdate").asc).persist()
    val cnt5 = df5.count
    val finish5 = System.nanoTime
    val timeElapsed5 = (finish5 - start5) / 1e6.toLong
    println(s"\tElapsed time ${timeElapsed5} ms, selected ${cnt5} records")
    TestTimes.update(4, (TestTimes(4) + timeElapsed5))
  }

  for (i <- TestTimes.indices){
    println(s"${i+1} ${TestTimes(i) / TEST_REPEAT_COUNT}")
  }
  spark.stop()
}