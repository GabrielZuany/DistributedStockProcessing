import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import java.nio.file.{Files, Paths}

object Main {

  /* Function to calculate the EMA.
  * The Exponential Moving Average (EMA) is a type of moving average that gives more weight to recent prices,
  * making it more responsive to new information compared to the Simple Moving Average (SMA).
  * It is used to identify trends in a stock or asset's price by smoothing out price fluctuations over a specified period,
  * and can be used to generate buy and sell signals.
  */
  private def computeEMA(df: DataFrame, windowSizes: List[Int]): DataFrame = {
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

  /*
  * Function to calculate RSI
  * The Relative Strength Index (RSI) is a technical indicator that measures the speed and magnitude of price changes.
  * It is used to identify overbought and oversold conditions, and to generate buy and sell signals.
  */
  private def computeRSI(df: DataFrame, period: Int = 14): DataFrame = {
    val windowSpec = Window.orderBy("Date")

    // Calculate the daily price change
    val dfWithChange = df.withColumn("PriceChange", col("Close") - lag(col("Close"), 1).over(windowSpec))

    // Separate the gains and losses
    val dfWithGainsAndLosses = dfWithChange
      .withColumn("Gain", when(col("PriceChange") > 0, col("PriceChange")).otherwise(0))
      .withColumn("Loss", when(col("PriceChange") < 0, -col("PriceChange")).otherwise(0))

    // Calculate the average gain and average loss over the window period
    val avgGain = avg("Gain").over(windowSpec.rowsBetween(-period + 1, 0))
    val avgLoss = avg("Loss").over(windowSpec.rowsBetween(-period + 1, 0))

    // Calculate RS (Relative Strength)
    val dfWithRS = dfWithGainsAndLosses.withColumn("AvgGain", avgGain).withColumn("AvgLoss", avgLoss)
      .withColumn("RS", col("AvgGain") / col("AvgLoss"))

    import org.apache.spark.sql.functions._
    // Calculate the RSI
    val dfWithRSI = dfWithRS.withColumn("RSI", lit(100) - (lit(100) / (lit(1) + col("RS"))))

    dfWithRSI
  }

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.sql.execution").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("StockAnalysis")
//      .master("spark://spark-master:7077")
      .master("local[*]") // Use "local[*]" para rodar localmente
      .config("spark.driver.bindAddress", "127.0.0.1") // Evita erro de conexão no Docker
      .getOrCreate()

    // List all CSV files in the directory
    val dataDir = "data/tiny"
    val files = Files.walk(Paths.get(dataDir))
      .filter(Files.isRegularFile(_))
      .filter(_.toString.endsWith(".csv"))
      .toArray
      .map(_.toString)
      .toList

    println(s"${files.length} found to process...")

    var dataframes:List[DataFrame] = List()

    var start = System.nanoTime
    // Process each file independently
    files.foreach { file =>
      println(s"Processing file: $file")
      val df = spark.read.option("header", "true").option("inferSchema", "true").csv(file)

      // process file
      var processedDf = computeEMA(df, List(10, 20, 50, 100))
      processedDf = computeRSI(processedDf) // Calculate RSI

      // Append the processed DataFrame to the list
      dataframes = processedDf :: dataframes
    }

    println(s"Execution time: ${(System.nanoTime - start) / 1e9d} s\n-----------\n")
    dataframes.foreach { df=>
      df.show(10)
    }
  }
}
