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

| Date       | Open               | High               | Low                | Close              | Adj Close           | Volume   | EMA_10             | EMA_20              | EMA_50              | EMA_100             | PriceChange          | Gain                 | Loss | AvgGain              | AvgLoss              | RS                  | RSI                |
|------------|--------------------|--------------------|--------------------|--------------------|---------------------|----------|--------------------|---------------------|---------------------|---------------------|----------------------|----------------------|------|----------------------|----------------------|---------------------|--------------------|
| 1980-12-19 | 0.5044642686843872 | 0.5066964030265808 | 0.5044642686843872 | 0.5044642686843872 | 0.3997070789337158  | 12157600 | 0.4807224029844457 | 0.47821003624371117 | 0.4765843871761771  | 0.47602104343990287 | 0.02901783585548401  | 0.02901783585548401  | 0.0  | 0.008928567171096802 | 0.010416666666666666 | 0.857142448425293   | 46.15383430345263  |
| 1980-12-22 | 0.5290178656578064 | 0.53125            | 0.5290178656578064 | 0.5290178656578064 | 0.4191618859767914  | 9340800  | 0.5089285590431907 | 0.5068027064913795  | 0.5054271548402076  | 0.5049504785254451  | 0.02455359697341919  | 0.02455359697341919  | 0.0  | 0.011160714285714286 | 0.008928571428571428 | 1.25                | 55.55555555555556  |
| 1980-12-23 | 0.5513392686843872 | 0.5535714030265808 | 0.5513392686843872 | 0.5513392686843872 | 0.4368479549884796  | 11737600 | 0.5330763025717302 | 0.5311437135650998  | 0.5298932147961036  | 0.5294598736385308  | 0.02232140302658081  | 0.02232140302658081  | 0.0  | 0.012555800378322601 | 0.0078125            | 1.607142448425293   | 61.64382960340363  |
| 1980-12-17 | 0.4620535671710968 | 0.4642857015132904 | 0.4620535671710968 | 0.4620535671710968 | 0.3661033511161804  | 21610400 | 0.4529220841147683 | 0.4519557896114531  | 0.45133054022695507 | 0.4511138696481686  | 0.011160701513290405 | 0.011160701513290405 | 0.0  | 0.002790175378322196 | 0.015625             | 0.17857122421264648 | 15.151500439181547 |  |
| 1980-12-18 | 0.4754464328289032 | 0.4776785671710968 | 0.4754464328289032 | 0.4754464328289032 | 0.37671515345573425 | 18362400 | 0.4644886336543343 | 0.463329078186126   | 0.46257877758904997 | 0.46231877243164743 | 0.013392865657806396 | 0.013392865657806396 | 0.0  | 0.004910713434219361 | 0.0125               | 0.39285707473754883 | 28.205124693901098 |
| ....       | ....               | ....               | ....               | ....               | ....                | ....     | ....               | ....                | ....                | ....                | ....                 | ....                 | .... | ....                 | ....                 | ....                | ....               |


## Conclusion
This project provides a simple framework for calculating important technical indicators (EMA and RSI) for 
stock market analysis using Apache Spark. The application is scalable and can process large volumes of data distributed 
across multiple files.

## Author
- [Gabriel Zuany Duarte Vargas](https://github.com/GabrielZuany) and [Lorenzo Rizzi Fiorot](https://github.com/lololoco159159159)