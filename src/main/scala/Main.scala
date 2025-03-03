import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import java.nio.file.{Files, Paths}


object Main {

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

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.sql.execution").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("StockAnalysis")
      .master("spark://spark-master:7077")
//      .master("local[*]") // Use "local[*]" para rodar localmente
      .config("spark.driver.bindAddress", "127.0.0.1") // Evita erro de conexão no Docker
      .getOrCreate()

    // List all CSV files in the directory
    val dataDir = "data/small"
    val files = Files.walk(Paths.get(dataDir))
      .filter(Files.isRegularFile(_))
      .filter(_.toString.endsWith(".csv"))
      .toArray
      .map(_.toString)
      .toList

    println(s"${files.length} found to process...")

    // Process each file independently
    files.foreach { file =>
      println(s"Processing file: $file")

      // load file
      var start = System.nanoTime()
      val df = spark.read.option("header", "true").option("inferSchema", "true").csv(file)
      println(s"Time taken to load file in memory: ${(System.nanoTime() - start) / 1e9} s")

      // process file
      start = System.nanoTime()
      val processedDf = computeEMA(df, List(10, 20, 50, 100))
      println(s"Time taken to compute EMA: ${(System.nanoTime() - start) / 1e9} s")

      processedDf.show(5)
    }
  }
}
