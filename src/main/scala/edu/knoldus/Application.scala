package edu.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * Created by pallavi on 27/2/18.
 */
object Application extends App {

  Logger.getLogger("org").setLevel(Level.OFF)


  val Conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Spark-Application")


  val spark = SparkSession
    .builder()
    .config(Conf)
    .getOrCreate()

  //Question 1
  val readcsv = spark.read.option("header", true).option("inferSchema", true)
    .csv("/home/pallavi/WorkSpace/spark01-assignment03/src/main/resources/data.csv")
  readcsv.show

  val df = readcsv.withColumn("matchPlayedAsHome", lit(1)).withColumn("matchPlayedAsAway", lit(1))
    .withColumn("winsHome", lit(1)).withColumn("winsAway", lit(1))

  //Question 2
  df.createOrReplaceTempView("MatchData")
  val sqlDF = spark
    .sql("SELECT HomeTeam, sum(matchPlayedAsHome) AS played_1 FROM MatchData GROUP BY HomeTeam")
  sqlDF.show()
  sqlDF.createOrReplaceTempView("playHome")


  val sqlDFAway = spark
    .sql("SELECT AwayTeam, sum(matchPlayedAsAway) AS played_2 FROM MatchData GROUP BY AwayTeam")
  sqlDFAway.createOrReplaceTempView("playAway")


  val joinPlayed = spark
    .sql("SELECT * FROM playHome JOIN playAway ON playHome.HomeTeam = playAway.AwayTeam")
  joinPlayed.createOrReplaceTempView("PlaysTable")
  val sumTotalPlay = spark
    .sql("SELECT HomeTeam AS Team , (played_1 + played_2) As Total_Played FROM PlaysTable")
  sumTotalPlay.createOrReplaceTempView("PlaysTotalTable")


  val winHome = spark
    .sql(
      "SELECT HomeTeam, sum(winsHome) AS sum_wins1 FROM MatchData WHERE FTR = 'H' GROUP BY " +
      "HomeTeam")
  winHome.createOrReplaceTempView("winHome")
  val winAway = spark
    .sql(
      "SELECT AwayTeam, sum(winsAway) AS sum_wins2 FROM MatchData where FTR = 'A' GROUP BY " +
      "AwayTeam")
  winAway.createOrReplaceTempView("winAway")


  val joinTeams = spark
    .sql("SELECT * FROM winAway JOIN winHome ON winHome.HomeTeam = winAway.AwayTeam")
  joinTeams.createOrReplaceTempView("WinsTable")

  joinTeams.printSchema()
  val sumWins = spark
    .sql("SELECT HomeTeam AS Team , (sum_wins1+ sum_wins2) As Total_win FROM WinsTable")
  sumWins.createOrReplaceTempView("WinsSumTable")

  val percentageJoin = spark
    .sql(
      "SELECT WinsSumTable.Team, Total_win, Total_Played, (Total_win/Total_Played*100) AS " +
      "Percentage  FROM WinsSumTable JOIN PlaysTotalTable ON WinsSumTable.Team = PlaysTotalTable" +
      ".Team order by Percentage DESC LIMIT 10")
    .show()

  import spark.implicits._


  //Question 4
  val matchDataSet = readcsv
    .map(row => MatchData(row.getAs[String]("HomeTeam"),
      row.getAs[String]("AwayTeam"),
      row.getAs[Int]("FTHG"),
      row.getAs[Int]("FTAG"),
      row.getAs[String]("FTR")
    ))


  matchDataSet.show()

  //Question 5

  val home = matchDataSet.groupBy("HomeTeam").count().withColumnRenamed("count", "HomeCount")
  val away = matchDataSet.groupBy("AwayTeam").count().withColumnRenamed("count", "AwayCount")
  val totalMatchPlayed = home.join(away, home.col("HomeTeam") === away.col("AwayTeam"))
    .map(row => MatchesPlayed(row.getAs("HomeTeam"),
      row.getAs[Long]("HomeCount") + row.getAs[Long]("AwayCount"))).show()


  //Question 6
  val homeWins =
    matchDataSet.filter(x => x.FTR == "H").groupBy("HomeTeam").count() withColumnRenamed
    ("count", "HomeWinCount")
  val awayWins =
    matchDataSet.filter(x => x.FTR == "A").groupBy("AwayTeam").count() withColumnRenamed
    ("count", "AwayWinCount")
  val totalWin = homeWins.join(awayWins, homeWins.col("HomeTeam") === awayWins.col("AwayTeam"))
    .map(row => MatchesWon(row.getAs("HomeTeam"),
      row.getAs[Long]("HomeWinCount") + row.getAs[Long]("AwayWinCount"))).orderBy(desc("Count_Win"))
    .limit(10).show()


}
