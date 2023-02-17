package com.spark.scala_benchmark

import org.apache.spark.sql.api.java.UDF4
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import java.sql.Date
import java.util.{ArrayList, HashMap, List}
import java.util.function.Function
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

  /**
   * measureQueryExecutionTime - measures query execution time
   *
   * @param spark       - current spark session
   * @param parquetPath - path to parquet files directory (input data)
   * @param func        - query applied to a dataset
   * @return hashmap with query execution time and number of records in the query result dataframe
   */
  def measureQueryExecutionTime(spark: SparkSession, parquetPath: String,
                                func: Function[Dataset[Row], Dataset[Row]]): mutable.HashMap[String, Long] = {
    spark.catalog.clearCache()
    val start = System.nanoTime
    val df = func.apply(readParquetDataset(spark, parquetPath))
    val recordsCount = df.count
    val finish = System.nanoTime
    val executionTime = (finish - start) / 1e6.toLong
    mutable.HashMap("execution_time" -> executionTime, "records_count" -> recordsCount)
  }


  val spark = SparkSession.builder()
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .appName("SparkScalaTest")
    .master("local[8]")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  //preprocessParquetDataset(spark, csvPath, parquetPath)
  var TestTimes = Array.fill[Long](TEST_TYPES_COUNT)(0)
  var start = System.nanoTime
  val opensky = readParquetDataset(spark, PARQUET_PATH).persist
  var cnt = opensky.count
  var finish = System.nanoTime
  var executionTime = (finish - start) / 1e6.toLong
  println("Test reading dataset")
  println(s"Elapsed time ${executionTime} ms, selected ${cnt} records")
  println("Dataset schema")
  opensky.printSchema()
  //  print(spark.conf.get("spark.submit.deployMode"))
  spark.catalog.clearCache

  val spark_queries = new mutable.ListBuffer[Function[Dataset[Row], Dataset[Row]]]
  spark_queries += ((df: Dataset[Row]) =>
    df.filter(functions.col("origin").isin("UUEE", "UUDD", "UUWW"))
      .filter(functions.to_date(functions.col("firstseen"), "YYYY-MM-DD").geq(Date.valueOf("2020-06-01")))
      .filter(functions.to_date(functions.col("firstseen"), "YYYY-MM-DD").leq(Date.valueOf("2020-08-31")))
      .persist)
  spark_queries += ((df: Dataset[Row]) =>
    df.withColumn("flighttime", functions.col("lastseen").cast(DataTypes.LongType)
      .minus(functions.col("firstseen").cast(DataTypes.LongType)))
    .groupBy(functions.col("origin"), functions.col("destination"))
    .agg(functions.avg("flighttime").alias("AvgFlightTime"))
    .orderBy(functions.col("AvgFlightTime").desc).persist)
  val ws3 = Window.orderBy(functions.monotonically_increasing_id).rowsBetween(Long.MinValue, Long.MaxValue)
  spark_queries += ((df: Dataset[Row]) =>
    df.withColumn("flighttime", functions.col("lastseen").cast(DataTypes.LongType)
      .minus(functions.col("firstseen").cast(DataTypes.LongType)))
    .groupBy(functions.col("origin"), functions.col("destination"))
    .agg(functions.round(functions.avg("flighttime"), DOUBLE_PRECISION).alias("AvgFlightTime"))
    .withColumn("TotalAvgFlightTime", functions.round(functions.avg("AvgFlightTime").over(ws3)
      .cast(DataTypes.DoubleType), DOUBLE_PRECISION))
    .filter(functions.col("AvgFlightTime").geq(functions.col("TotalAvgFlightTime")))
    .orderBy(functions.col("AvgFlightTime").desc).persist)
  val ws4 = Window.orderBy(functions.col("flightdate")).rowsBetween(Window.currentRow - WINDOW_SIZE_IN_DAYS, Window.currentRow)
  spark_queries += ((df: Dataset[Row]) =>
    df.withColumn("distance_km", functions.round(functions.lit(2 * EARTH_RADIUS)
      .multiply(functions.asin(functions.sqrt(functions.pow(functions.sin(functions.col("latitude_2")
        .divide(functions.lit(DEG2RAD)).minus(functions.col("latitude_1").divide(functions.lit(DEG2RAD)))
        .divide(functions.lit(2.0))), 2.0).plus(functions.cos(functions.col("latitude_1")
        .divide(functions.lit(DEG2RAD))).multiply(functions.cos(functions.col("latitude_2")
        .divide(functions.lit(DEG2RAD)))).multiply(functions.pow(functions.sin(functions.col("longitude_2")
        .divide(functions.lit(DEG2RAD)).minus(functions.col("longitude_1")
        .divide(functions.lit(DEG2RAD))).divide(functions.lit(2.0))), 2.0)))))), 0)
      .divide(functions.lit(1000.0))).withColumn("flightdate", functions.to_date(functions.col("firstseen")))
      .groupBy(functions.col("flightdate"))
      .agg(functions.max("distance_km").as("max_distance_km"))
      .select(functions.col("flightdate"), functions.max(functions.col("max_distance_km")).over(ws4)
        .as("max_distance_km_on_date")).orderBy(functions.col("flightdate").asc).persist)


  val get_distance = (latitude_1: Double, longitude_1: Double, latitude_2: Double, longitude_2: Double) =>
    2 * EARTH_RADIUS * Math.asin(Math.sqrt(Math.pow(Math.sin((Math.toRadians(latitude_2)
      - Math.toRadians(latitude_1)) / 2.0), 2.0) + Math.cos(Math.toRadians(latitude_1))
      * Math.cos(Math.toRadians(latitude_2)) * Math.pow(Math.sin((Math.toRadians(longitude_2)
      - Math.toRadians(longitude_1)) / 2.0), 2.0))).round / 1000.0
  spark.udf.register("get_distance", get_distance)
  spark_queries += ((df: Dataset[Row]) =>
    df.withColumn("distance_km",
      functions.callUDF("get_distance",
        functions.col("latitude_1"), functions.col("longitude_1"),
        functions.col("latitude_2"), functions.col("longitude_2")))
      .withColumn("flightdate", functions.to_date(functions.col("firstseen")))
      .groupBy(functions.col("flightdate")).agg(functions.max("distance_km")
      .as("max_distance_km")).select(functions.col("flightdate"),
      functions.max(functions.col("max_distance_km")).over(ws4).as("max_distance_km_on_date"))
      .orderBy(functions.col("flightdate").asc).persist)
  for (i <- 0 until TEST_REPEAT_COUNT) {
    println(s"${i+1} iteration")
    for (j <- spark_queries.indices) {
      val spark_query = spark_queries(j)
      val res = measureQueryExecutionTime(spark, PARQUET_PATH, spark_query)
      val recordsCount = res.getOrElse("records_count", 0L)
      val executionTime = res.getOrElse("execution_time", 0L)
      TestTimes.update(j, (TestTimes(j) + executionTime))
      println(s"\t${j + 1}-st query: elapsed time ${executionTime} ms, selected ${recordsCount} records")
    }
  }
  println("SUMMARY")
  for (i <- TestTimes.indices){
    println(s"\t${i+1} ${TestTimes(i) / TEST_REPEAT_COUNT} ms")
  }
  println("All tests completed successfully")
  spark.stop()
}