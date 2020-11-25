/*
1. Which country had the hottest average mean temperature over the year?
Answer: DJIBOUTI

2. Which country had the most consecutive days of tornadoes/funnel cloud
formations?
Answer: AUSTRALIA

3. Which country had the second highest average mean wind speed over the year?
Answer: ARUBA
 */

import org.apache.spark.sql.{DataFrame, SparkSession}

object CodingChallenge {

  lazy val sparkSession: SparkSession = {
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val sparkSession = SparkSession
      .builder()
      .appName("CodingChallengeSparkSession")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    //set new runtime options
    sparkSession.conf.set("spark.sql.shuffle.partitions", 6)
    sparkSession.conf.set("spark.executor.memory", "2g")

    sparkSession
  }

  def getData: DataFrame = {

    val dataPath =
      "/Users/tpeng/pve/hadoop-role-skeleton/component/pval/3490_queued_status/recommendation/BatchIncremental/src/test/resources/paytmteam-de-weather-challenge-beb4fc53605c"

    val stationListDF: DataFrame =
      sparkSession.read.option("header", true).csv(s"${dataPath}/stationlist.csv").cache

    println("stationListDF:")
    stationListDF.show
    stationListDF.printSchema
    stationListDF.createOrReplaceTempView("station_list")

    val countryListDF: DataFrame =
      sparkSession.read.option("header", true).csv(s"${dataPath}/countrylist.csv").cache

    println("countryListDF:")
    countryListDF.show
    countryListDF.printSchema
    countryListDF.createOrReplaceTempView("country_list")

    val stationFullDF: DataFrame = sparkSession
      .sql(s"""
              |SELECT A.STN_NO, A.COUNTRY_ABBR, B.COUNTRY_FULL
              |FROM station_list A
              |LEFT JOIN country_list B
              |ON A.COUNTRY_ABBR = B.COUNTRY_ABBR
              |""".stripMargin)

    println("stationFullDF:")
    stationFullDF.show
    stationFullDF.createOrReplaceTempView("station_full_data")

    val weatherDataFiles: Array[String] =
      Array(
        "part-00000-890686c0-c142-4c69-a744-dfdc9eca7df4-c000.csv",
        "part-00001-890686c0-c142-4c69-a744-dfdc9eca7df4-c000.csv",
        "part-00002-890686c0-c142-4c69-a744-dfdc9eca7df4-c000.csv",
        "part-00003-890686c0-c142-4c69-a744-dfdc9eca7df4-c000.csv",
        "part-00004-890686c0-c142-4c69-a744-dfdc9eca7df4-c000.csv"
      )

    val weatherDFs: Array[DataFrame] = weatherDataFiles.map(fileName => {
      sparkSession.read
        .option("header", true)
        .csv(s"${dataPath}/data/2019/${fileName}")
    })

    val weatherDF: DataFrame = weatherDFs.reduce(_ union _).cache

    println(s"weatherDF_size = ${weatherDF.count}")
    weatherDF.show
    weatherDF.createOrReplaceTempView("weather_data")

    val weatherFullDF: DataFrame = sparkSession
      .sql(s"""
              |SELECT A.*, B.COUNTRY_FULL
              |FROM weather_data A
              |LEFT JOIN station_full_data B
              |ON A.`STN---` = B.STN_NO
              |""".stripMargin)
      .cache

    println(s"weatherFullDF_size = ${weatherFullDF.count}")
    weatherFullDF.show
    weatherFullDF.printSchema

    weatherFullDF
  }

  def fillMissingValue(weatherFullDF: DataFrame): DataFrame = {
    weatherFullDF.createOrReplaceTempView("weather_full_data")

    val weatherMissingReplacedWith0DF: DataFrame = sparkSession
      .sql(s"""
              |SELECT CAST(`STN---` AS INT) AS STN,
              |CAST(COUNTRY_FULL AS STRING) AS COUNTRY_FULL,
              |CAST(WBAN AS INT) AS WBAN,
              |CAST(YEARMODA AS INT) AS YEARMODA,
              |CAST(IF(TEMP == '9999.9' OR TEMP IS NULL, '0.0', TEMP) AS DOUBLE) AS TEMP,
              |CAST(IF(DEWP == '9999.9' OR DEWP IS NULL, '0.0', DEWP) AS DOUBLE) AS DEWP,
              |CAST(IF(SLP == '9999.9' OR SLP IS NULL, '0.0', SLP) AS DOUBLE) AS SLP,
              |CAST(IF(STP == '9999.9' OR STP IS NULL, '0.0', STP) AS DOUBLE) AS STP,
              |CAST(IF(VISIB == '999.9' OR VISIB IS NULL, '0.0', VISIB) AS DOUBLE) AS VISIB,
              |CAST(IF(WDSP == '999.9' OR WDSP IS NULL, '0.0', WDSP) AS DOUBLE) AS WDSP,
              |CAST(IF(MXSPD == '999.9' OR MXSPD IS NULL, '0.0', MXSPD) AS DOUBLE) AS MXSPD,
              |CAST(IF(GUST == '999.9' OR GUST IS NULL, '0.0', GUST) AS DOUBLE) AS GUST,
              |CAST(IF(MAX == '9999.9' OR MAX IS NULL, '0.0', MAX) AS DOUBLE) AS MAX,
              |CAST(IF(MIN == '9999.9' OR MIN IS NULL, '0.0', MIN) AS DOUBLE) AS MIN,
              |CAST(IF(PRCP == '99.99' OR PRCP IS NULL, '0.0', PRCP) AS DOUBLE) AS PRCP,
              |CAST(IF(SNDP == '999.9' OR SNDP IS NULL, '0.0', SNDP) AS DOUBLE) AS SNDP,
              |CAST(FRSHTT AS STRING) AS FRSHTT
              |FROM weather_full_data
              |""".stripMargin)

    println("weatherMissingReplacedWith0DF:")
    weatherMissingReplacedWith0DF.show
    weatherMissingReplacedWith0DF.printSchema
    weatherMissingReplacedWith0DF.createOrReplaceTempView(
      "weather_missing_repalced_with_0"
    )

    val meanDF: DataFrame = sparkSession
      .sql(s"""
              |SELECT 
              |AVG(TEMP) AS AVG_TEMP,
              |AVG(DEWP) AS AVG_DEWP,
              |AVG(SLP) AS AVG_SLP,
              |AVG(STP) AS AVG_STP,
              |AVG(VISIB) AS AVG_VISIB,
              |AVG(WDSP) AS AVG_WDSP,
              |AVG(MXSPD) AS AVG_MXSPD,
              |AVG(GUST) AS AVG_GUST,
              |AVG(MAX) AS AVG_MAX,
              |AVG(MIN) AS AVG_MIN,
              |AVG(PRCP) AS AVG_PRCP,
              |AVG(SNDP) AS AVG_SNDP
              |FROM weather_missing_repalced_with_0
              |""".stripMargin)

    meanDF.show

    val avgTemp: Double = meanDF.collect.head.getDouble(0)
    val avgDEWP: Double = meanDF.collect.head.getDouble(1)
    val avgSLP: Double = meanDF.collect.head.getDouble(2)
    val avgSTP: Double = meanDF.collect.head.getDouble(3)
    val avgVISIB: Double = meanDF.collect.head.getDouble(4)
    val avgWDSP: Double = meanDF.collect.head.getDouble(5)
    val avgMXSPD: Double = meanDF.collect.head.getDouble(6)
    val avgGUST: Double = meanDF.collect.head.getDouble(7)
    val avgMAX: Double = meanDF.collect.head.getDouble(8)
    val avgMIN: Double = meanDF.collect.head.getDouble(9)
    val avgPRCP: Double = meanDF.collect.head.getDouble(10)
    val avgSNDP: Double = meanDF.collect.head.getDouble(11)

    val weatherMissingFilledDF: DataFrame = sparkSession
      .sql(s"""
              |SELECT CAST(`STN---` AS INT) AS STN,
              |CAST(COUNTRY_FULL AS STRING) AS COUNTRY_FULL,
              |CAST(WBAN AS INT) AS WBAN,
              |CAST(YEARMODA AS INT) AS YEARMODA,
              |CAST(IF(TEMP == '9999.9' OR TEMP IS NULL, '$avgTemp', TEMP) AS DOUBLE) AS TEMP,
              |CAST(IF(DEWP == '9999.9' OR DEWP IS NULL, '$avgDEWP', DEWP) AS DOUBLE) AS DEWP,
              |CAST(IF(SLP == '9999.9' OR SLP IS NULL, '$avgSLP', SLP) AS DOUBLE) AS SLP,
              |CAST(IF(STP == '9999.9' OR STP IS NULL, '$avgSTP', STP) AS DOUBLE) AS STP,
              |CAST(IF(VISIB == '999.9' OR VISIB IS NULL, '$avgVISIB', VISIB) AS DOUBLE) AS VISIB,
              |CAST(IF(WDSP == '999.9' OR WDSP IS NULL, '$avgWDSP', WDSP) AS DOUBLE) AS WDSP,
              |CAST(IF(MXSPD == '999.9' OR MXSPD IS NULL, '$avgMXSPD', MXSPD) AS DOUBLE) AS MXSPD,
              |CAST(IF(GUST == '999.9' OR GUST IS NULL, '$avgGUST', GUST) AS DOUBLE) AS GUST,
              |CAST(IF(MAX == '9999.9' OR MAX IS NULL, '$avgMAX', MAX) AS DOUBLE) AS MAX,
              |CAST(IF(MIN == '9999.9' OR MIN IS NULL, '$avgMIN', MIN) AS DOUBLE) AS MIN,
              |CAST(IF(PRCP == '99.99' OR PRCP IS NULL, '$avgPRCP', PRCP) AS DOUBLE) AS PRCP,
              |CAST(IF(SNDP == '999.9' OR SNDP IS NULL, '$avgSNDP', SNDP) AS DOUBLE) AS SNDP,
              |CAST(FRSHTT AS STRING) AS FRSHTT
              |FROM weather_full_data
              |""".stripMargin)

    println("weatherMissingFilledDF:")
    weatherMissingFilledDF.show
    weatherMissingFilledDF.printSchema
    weatherMissingFilledDF
  }

  def getStatistics(weatherMissingFilledDF: DataFrame): Unit = {
    weatherMissingFilledDF.createOrReplaceTempView(
      "weather_missing_filled"
    )

    //1. Which country had the hottest average mean temperature over the year?

    val countryTempDF: DataFrame = sparkSession
      .sql(s"""
              |SELECT COUNTRY_FULL, AVG_TEMP
              |FROM
              |  (SELECT COUNTRY_FULL, AVG(TEMP) AS AVG_TEMP
              |  FROM weather_missing_filled
              |  GROUP BY COUNTRY_FULL) A
              |ORDER BY AVG_TEMP DESC
              |LIMIT 1
              |""".stripMargin)

    println("countryTempDF:")
    countryTempDF.show

    /*
    +------------+-----------------+
    |COUNTRY_FULL|         AVG_TEMP|
    +------------+-----------------+
    |    DJIBOUTI|90.06114457831325|
    +------------+-----------------+
     */
    //Answer: DJIBOUTI has the hottest average mean temperature over the year

    //2. Which country had the most consecutive days of tornadoes/funnel cloud formations?

    val tornadoesDF: DataFrame = sparkSession
      .sql(s"""
              |SELECT COUNTRY_FULL, YEARMODA, FRSHTT, SUBSTR(FRSHTT, 6, 1) AS TORNADOES
              |FROM weather_missing_filled
              |""".stripMargin)

    println("tornadoesDF:")
    tornadoesDF.show
    tornadoesDF.createOrReplaceTempView("tornadoes_df")

    val consecutiveTornadoesDF: DataFrame = sparkSession
      .sql(s"""
              |SELECT *,
              |       (ROW_NUMBER() OVER (ORDER BY COUNTRY_FULL, YEARMODA) - ROW_NUMBER() OVER (PARTITION BY TORNADOES ORDER BY COUNTRY_FULL, YEARMODA)) AS CONSECUTIVE_TORNADOES
              |FROM tornadoes_df
              |""".stripMargin)

    println("consecutiveTornadoesDF:")
    consecutiveTornadoesDF.show
    consecutiveTornadoesDF.createOrReplaceTempView("consecutive_tornadoes")

    val consecutiveTornadoesCountDF: DataFrame = sparkSession
      .sql(s"""
              |SELECT COUNTRY_FULL, COUNT
              |FROM
              |  (SELECT COUNTRY_FULL, CONSECUTIVE_TORNADOES, COUNT(1) AS COUNT
              |  FROM consecutive_tornadoes
              |  GROUP BY COUNTRY_FULL, CONSECUTIVE_TORNADOES) A
              |ORDER BY COUNT DESC
              |LIMIT 1
              |""".stripMargin)

    println("consecutiveTornadoesCountDF:")
    consecutiveTornadoesCountDF.show

    /*
    +------------+------+
    |COUNTRY_FULL| COUNT|
    +------------+------+
    |   AUSTRALIA|180842|
    +------------+------+
     */

    //Answer: AUSTRALIA had the most consecutive days of tornadoes/funnel cloud formations?

    //3. Which country had the second highest average mean wind speed over the year?

    val countryWindSpeedDF: DataFrame = sparkSession
      .sql(s"""
              |SELECT COUNTRY_FULL, AVG_WDSP
              |FROM
              |  (SELECT COUNTRY_FULL, AVG(WDSP) AS AVG_WDSP
              |  FROM weather_missing_filled
              |  GROUP BY COUNTRY_FULL) A
              |ORDER BY AVG_WDSP DESC
              |LIMIT 2
              |""".stripMargin)

    println("countryWindSpeedDF:")
    countryWindSpeedDF.show

    /*
    +--------------------+------------------+
    |        COUNTRY_FULL|          AVG_WDSP|
    +--------------------+------------------+
    |FALKLAND ISLANDS ...| 17.85438437167519|
    |               ARUBA|15.975683060109283|
    +--------------------+------------------+

     */
    //Answer: ARUBA has the second highest average mean wind speed over the year

  }

  def main(args: Array[String]) {
    val weatherFullDF = getData
    val weatherFilledMissingDF = fillMissingValue(weatherFullDF)
    getStatistics(weatherFilledMissingDF)
  }
}
