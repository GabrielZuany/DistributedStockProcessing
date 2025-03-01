import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}

import java.nio.file.{Files, Paths}


object Main {

  def computeEMA(df: DataFrame, windowSizes: List[Int]): DataFrame = {
    val windowSpec = Window.orderBy("Date")

    // Criar as colunas EMA_X inicialmente com o valor de "Close"
    var resultDf = windowSizes.foldLeft(df) { (tempDf, window) =>
      tempDf.withColumn(s"EMA_$window", col("Close"))
    }

    // Aplicar o cálculo da EMA em todas as colunas de uma vez
    for (window <- windowSizes) {
      val alpha = 2.0 / (window + 1)
      val emaCol = s"EMA_$window"

      resultDf = resultDf.withColumn(
        emaCol,
        when(lag(col(emaCol), 1).over(windowSpec).isNull, col("Close")) // Primeiro valor = Close
          .otherwise((col("Close") * alpha) + (lag(col(emaCol), 1).over(windowSpec) * (1 - alpha)))
      )
    }
    resultDf
  }

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("StockAnalysis")
      .master("local[*]") // Use "local[*]" para rodar localmente
      .config("spark.driver.bindAddress", "127.0.0.1") // Evita erro de conexão no Docker
      .getOrCreate()

    println("Iniciando carregamento dos dados...")

    // List all CSV files in the directory
    val dataDir = "data/small"
    val files = Files.walk(Paths.get(dataDir))
      .filter(Files.isRegularFile(_))
      .filter(_.toString.endsWith(".csv"))
      .toArray
      .map(_.toString)
      .toList

    // Process each file independently
    files.foreach { file =>
      println(s"Processando o arquivo: $file")

      val loadStart = System.nanoTime()
      val df = spark.read.option("header", "true").option("inferSchema", "true").csv(file)
      val loadEnd = System.nanoTime()

      println(s"Tempo total de carregamento: ${(loadEnd - loadStart) / 1e9} ms")
      val runtime = Runtime.getRuntime
      println(s"Memoria usada: ${(runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)} MB")

      val emaStart = System.nanoTime()
      // Calcular EMA com diferentes períodos
      val processedDf = computeEMA(df, List(10, 20, 50, 100))
      val emaEnd = System.nanoTime()

      println(s"Tempo total de cálculo da EMA: ${(emaEnd - emaStart) / 1e9} ms")
      val runtime2 = Runtime.getRuntime
      println(s"Memoria usada após EMA: ${(runtime2.totalMemory() - runtime2.freeMemory()) / (1024 * 1024)} MB")

      // Exibir os primeiros resultados
      processedDf.show(5)

      // Salvar os resultados de cada arquivo individualmente
//      val outputPath = s"output/emadata_${Paths.get(file).getFileName.toString}"
//      processedDf.write.mode("overwrite").parquet(outputPath)
//      println(s"Resultado salvo em: $outputPath")
    }
  }
}
