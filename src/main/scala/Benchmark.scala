import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.lang.Thread.sleep
import java.nio.file.{Files, Paths}

class Benchmark(spark: SparkSession, dataDir: String) {

  def loadAndConcatenateData(): DataFrame = {
    // List all CSV files in the directory
    val files = Files.walk(Paths.get(dataDir))
      .filter(Files.isRegularFile(_))
      .filter(_.toString.endsWith(".csv"))
      .toArray
      .map(_.toString)
      .toList

    // Initialize df by reading the first file to get the schema
    val firstFile = files.headOption.getOrElse(throw new RuntimeException("No CSV files found"))
    var df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(firstFile)

    // Read and concatenate all CSV files into a single DataFrame
    files.tail.foreach { file =>
      val tempDf = spark.read.option("header", "true").option("inferSchema", "true").csv(file)
      df = df.union(tempDf)  // Concatenate the DataFrames
    }

    df
  }

  def computeEMA(df: DataFrame, windowSizes: List[Int]): DataFrame = {
    val windowSpec = Window.orderBy("Date")

    // Create the EMA columns initialized with the value of "Close"
    var resultDf = windowSizes.foldLeft(df) { (tempDf, window) =>
      tempDf.withColumn(s"EMA_$window", col("Close"))
    }

    // Apply the EMA calculation
    for (window <- windowSizes) {
      val alpha = 2.0 / (window + 1)
      val emaCol = s"EMA_$window"

      resultDf = resultDf.withColumn(
        emaCol,
        when(lag(col(emaCol), 1).over(windowSpec).isNull, col("Close")) // First value = Close
          .otherwise((col("Close") * alpha) + (lag(col(emaCol), 1).over(windowSpec) * (1 - alpha)))
      )
    }
    resultDf
  }

  def benchmarkProcessing(): Unit = {
    // Step 1: Load and concatenate the data
    println("Iniciando carregamento e concatenação dos dados...")
    val loadStart = System.nanoTime()
    val df = loadAndConcatenateData()
    val loadEnd = System.nanoTime()
    println(s"Rows: ${df.count()}")
    println(s"Tempo total de carregamento e concatenação: ${(loadEnd - loadStart) / 1e9} s")

    // Step 2: Calculate EMA
    println("Iniciando o cálculo da EMA...")
    val emaStart = System.nanoTime()
    val processedDf = computeEMA(df, List(10, 20, 50, 100))
    val emaEnd = System.nanoTime()

    println(s"Tempo total de cálculo da EMA: ${(emaEnd - emaStart) / 1e9} s")

    // Step 3: Show results
    processedDf.show(5)
  }
}

object BenchmarkApp {
  def main(args: Array[String]): Unit = {
    // Disable all logs
    Logger.getRootLogger.setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.sql.execution").setLevel(Level.ERROR)

    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("BenchmarkStockAnalysis")
      .master("local[*]") // Use "local[*]" for local execution
      .config("spark.driver.bindAddress", "127.0.0.1") // Avoid connection errors in Docker
      .getOrCreate()


    // Define the directory where CSV files are located
    val dataDir = "data/small"
    val fullDatasetDir = "data/full"

    // Initialize Benchmark and run the processing
    val benchmark = new Benchmark(spark, fullDatasetDir)
    benchmark.benchmarkProcessing()
//    sleep(999999999)
  }
}
