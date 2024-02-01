error id: file://<WORKSPACE>/src/main/scala/RawToFormatted.scala:[5333..5334) in Input.VirtualFile("file://<WORKSPACE>/src/main/scala/RawToFormatted.scala", "import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate

object RawToFormatted {
   val currentDate = LocalDate.now()

  def main(args: Array[String]): Unit = {
    val GCS_JSON_key = "<HOME>/Documents/projects/BigData/DataLake/utils/credentials/google_storage_secret_key_credentials.json"

    val spark = SparkSession.builder
      .appName("RawToFormatted")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration
      .set("google.cloud.auth.service.account.json.keyfile", GCS_JSON_key)

    if (args.length > 0) {
      val ROOT_BUCKET_PATH = args(0)
      this.formatedCompetition(spark, ROOT_BUCKET_PATH)
      this.formatedCompetitionMatches(spark,ROOT_BUCKET_PATH)
    } else {
      throw new RuntimeException("Provide arguments for google cloud bucket path")
    }
  }

  private def formatedCompetition(spark: SparkSession, root_bucket_path: String): Unit = {

    var competition_df = spark.read.option("multiline", value = true)
      .option("inferSchema", value = true)
      .json(s"$root_bucket_path/raw/football_data/competitions/2024-01-14/*.json")

    competition_df.printSchema

    competition_df = competition_df
      .withColumn("areaCode", col("area").getItem("code"))
      .withColumn("areaFlag", col("area").getItem("flag"))
      .withColumn("areaId", col("area").getItem("id"))
      .withColumn("areaName", col("area").getItem("name"))
      .withColumn("season", explode(col("seasons")))
      .withColumn("competitionId", col("id"))
      .withColumn("competitionCode", col("code"))
      .withColumn("currentSeasonId", col("currentSeason").getItem("id"))
      .withColumn("currentMatchDay", col("currentSeason").getItem("currentMatchDay"))

    competition_df = competition_df
      .withColumn("seasonId", col("season").getItem("id"))
      .withColumn("seasonWinnerId", col("season").getItem("winner").getItem("id"))

    var season_df = competition_df.select(col("season"), col("competitionId"), col("competitionCode"))
      .withColumn("id", col("season").getItem("id"))
      .withColumn("endDate", col("season").getItem("endDate"))
      .withColumn("startDate", col("season").getItem("startDate"))
      .withColumn("winnerTeamId", col("season").getItem("winner").getItem("id"))
      .withColumn("currentMatchDay", col("season").getItem("currentMatchDay"))
    season_df = season_df.drop("season")
    season_df.printSchema
    season_df.show()

    competition_df = competition_df.drop(col("seasons"))
    competition_df = competition_df.drop(col("season"))
    competition_df = competition_df.drop(col("currentSeason"))
    competition_df = competition_df.drop(col("area"))
    competition_df = competition_df.dropDuplicates("id")

    competition_df.printSchema()
    competition_df.show()

    competition_df.write.mode(SaveMode.Overwrite).parquet(s"$root_bucket_path/formatted/football_data/competition/" + currentDate)
    season_df.write.mode(SaveMode.Overwrite).parquet(s"$root_bucket_path/formatted/football_data/season/" + currentDate)
  }

  private def formatedCompetitionMatches(spark:SparkSession, root_bucket_path:String): Unit = {
    
    var competition_matches_df = spark.read
        .option("multiline", true).option("inferSchema", true).json(s"$root_bucket_path/raw/football_data/competition_matches/2024-01-14")
    
    competition_matches_df.printSchema()

    competition_matches_df = competition_matches_df
      .withColumn("match", explode(col("matches")))
      .withColumn("competitionId", col("competition").getItem("id"))

    var matches_df = competition_matches_df.drop("matches")
    matches_df = matches_df.drop("resultSet")
    matches_df = matches_df.drop("competition")
    matches_df = matches_df.drop("filters")

    matches_df = matches_df
      .withColumn("matchId", col("match").getItem("id"))
      .withColumn("awayTeamId", col("match").getItem("awayTeam").getItem("id"))
      .withColumn("homeTeamId", col("match").getItem("homeTeam").getItem("id"))
      .withColumn("group", col("match").getItem("group"))
      .withColumn("matchDay", col("match").getItem("matchday"))
      .withColumn("scoreDuration", col("match").getItem("score").getItem("duration"))
      .withColumn("scoreExtraTime", col("match").getItem("score").getItem("extraTime"))
      .withColumn("scoreFullTime", col("match").getItem("score").getItem("fullTime"))
      .withColumn("scoreHalfTime", col("match").getItem("score").getItem("halfTime"))
      .withColumn("penalties", col("match").getItem("score").getItem("penalties"))
      .withColumn("scoreRegularTime", col("match").getItem("score").getItem("regularTime"))
      .withColumn("winner", col("match").getItem("score").getItem("winner"))
      .withColumn("stage", col("match").getItem("stage"))
      .withColumn("date", col("match").getItem("utcDate"))
      .withColumn("status", col("match").getItem("status"))
      .withColumn("areaId", col("match").getItem("area").getItem("id"))

    matches_df = matches_df.drop("match")

    matches_df.printSchema()
    matches_df.select("matchId","homeTeamId","awayTeamId","competitionId","scoreFullTime").show()

    matches_df.write.mode(SaveMode.Overwrite).parquet(s"$root_bucket_path/formatted/football_data/matches/" + currentDate)
  }

  private def 
}
")
file://<WORKSPACE>/src/main/scala/RawToFormatted.scala
file://<WORKSPACE>/src/main/scala/RawToFormatted.scala:117: error: expected identifier; obtained rbrace
}
^
#### Short summary: 

expected identifier; obtained rbrace