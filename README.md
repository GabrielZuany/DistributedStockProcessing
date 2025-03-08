# Stock Analysis with Apache Spark

This project is a stock analysis application that uses Apache Spark to calculate technical indicators such as the Exponential Moving Average (EMA) and the Relative Strength Index (RSI). These indicators are commonly used in technical analysis to identify trends and overbought/oversold conditions in the stock market.

## Requirements

- Apache Spark (version 3.x recommended)
- Java JDK 8 or higher
- Scala (version 2.13)
- [CSV files](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset) containing stock data (closing price, date, etc.) 
- These CSV files must be in `data/` folder. It'll look like `data/stocks` and `data/etfs`.
- We suggest you to create another folder like `data/test` with a few files to test the application. If you run the `data/stocks` or `data/etfs`, please go get some coffee...
## Setup

1. **Install Apache Spark**:
    - Download and install Apache Spark from the [official website](https://spark.apache.org/downloads.html).
    - Configure the `SPARK_HOME` and `PATH` environment variables to include the Spark directory.
    - If you're using a good IDE such as IntelliJ, probably it will download these requirements for you.
2. **Compile the Project**:
    - Use SBT (Scala Build Tool) to compile the project:
      ```bash
      sbt compile
      ```

3. **Run the Project**:
    - Execute the project using SBT:
      ```bash
      sbt run
      ```

## Code Structure

The project consists of a single `Main` class containing the following main functions:

- **`computeEMA`**: Calculates the Exponential Moving Average (EMA) for different time windows (10, 20, 50, 100 days).
- **`computeRSI`**: Calculates the Relative Strength Index (RSI) for a default period of 14 days.
- **`main`**: The main function that reads CSV files containing stock data, processes each file to calculate the indicators, and displays the results.

## Usage Example

1. Place the CSV files containing stock data in the `data/<select subfolder>` directory.
2. Run the project as described in the Setup section.
3. The program will process each CSV file, calculate the technical indicators, and display the first 10 rows of each processed DataFrame.

## Sample Output

The program output will look similar to:

| Date       | Close              | EMA_10             | EMA_20             | EMA_50             | EMA_100             | RSI       |
|------------|--------------------|--------------------|--------------------|--------------------|---------------------|-----------|
| 1980-12-12 | 0.5133928656578064 | 0.515625           | 0.5133928656578064 | 0.5133928656578064 | 0.40678155422210693 | 117258400 |0.5133928656578064| 0.5133928656578064| 0.5133928656578064| 0.5133928656578064|                NULL|                 0.0|                 0.0|                 0.0|                 0.0|               NULL|              NULL|
| 1980-12-15 | 0.4888392984867096 | 0.4888392984867096 | 0.4866071343421936 | 0.4866071343421936 | 0.385558158159256   | 43971200  |0.5085227326913313|  0.510841843627748| 0.5123424448219001| 0.5128624551367051|-0.02678573131561...|                 0.0|0.026785731315612793|                 0.0|0.013392865657806396|                0.0|               0.0|
| 1980-12-16 | 0.453125           | 0.453125           | 0.4508928656578064 | 0.4508928656578064 | 0.3572602868080139  | 26432000  |0.4801136309450323|0.48320577541987103| 0.4852065747859431| 0.4858999211009186|-0.03571426868438721|                 0.0| 0.03571426868438721|                 0.0|0.020833333333333332|                0.0|               0.0|
| 1980-12-17 | 0.4620535671710968 | 0.4642857015132904 | 0.4620535671710968 | 0.4620535671710968 | 0.3661033511161804  | 21610400  |0.4529220841147683| 0.4519557896114531|0.45133054022695507| 0.4511138696481686|0.011160701513290405|0.011160701513290405|                 0.0|0.002790175378322...|            0.015625|0.17857122421264648|15.151500439181547|
| 1980-12-18 | 0.4754464328289032 | 0.4776785671710968 | 0.4754464328289032 | 0.4754464328289032 | 0.37671515345573425 | 18362400  |0.4644886336543343|  0.463329078186126|0.46257877758904997|0.46231877243164743|0.013392865657806396|0.013392865657806396|                 0.0|0.004910713434219361|              0.0125|0.39285707473754883|28.205124693901098|
| ...        | ...                | ...                | ...                | ...                | ...                 | ...       |

## Conclusion
This project provides a simple framework for calculating important technical indicators (EMA and RSI) for 
stock market analysis using Apache Spark. The application is scalable and can process large volumes of data distributed 
across multiple files.

## Author
- [Gabriel Zuany Duarte Vargas](https://github.com/GabrielZuany) and [Lorenzo Rizzi Fiorot](https://github.com/lololoco159159159)