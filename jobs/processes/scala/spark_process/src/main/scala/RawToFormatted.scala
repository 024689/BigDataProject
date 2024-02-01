import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate

object RawToFormatted {

  def main(args: Array[String]): Unit = {

    if (args.length > 0) {

        val spark = SparkSession.builder
          .appName("RawToFormatted")
          .getOrCreate()

        val typeExec = args(1) //  "local-spark" or "dataproc"

        if(typeExec == "local-spark"){
            val GCS_JSON_key = "/home/juniortemgoua0/DataLake/utils/credentials/google_storage_secret_key_credentials.json"
            spark.sparkContext.hadoopConfiguration
              .set("google.cloud.auth.service.account.json.keyfile", GCS_JSON_key)
        }

        val executionDate = spark.conf.get("spark.airflow.execution_date")
        println("************************************************")
        println(s"Date d'exécution  : $executionDate")
        println("************************************************")

      val ROOT_BUCKET_PATH = args(0)
      val currentDate: String = if(executionDate != null) executionDate else LocalDate.now().toString
      this.formattedCompetition(spark, ROOT_BUCKET_PATH,currentDate)
      this.formattedCompetitionMatches(spark, ROOT_BUCKET_PATH,currentDate)
      this.formattedTeams(spark, ROOT_BUCKET_PATH,currentDate)
    } else {
      throw new RuntimeException("Provide arguments for google cloud bucket path")
    }
  }

  private def formattedCompetition(spark: SparkSession, root_bucket_path: String, currentDate: String): Unit = {

    var competition_df = spark.read
      .option("multiline", value = true)
      .option("inferSchema", value = true)
      .json(s"$root_bucket_path/raw/football_data/competitions/$currentDate/*.json")

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

  private def formattedCompetitionMatches(spark:SparkSession, root_bucket_path:String,currentDate: String): Unit = {
    
    var competition_matches_df = spark.read
        .option("multiline", value = true)
        .option("inferSchema", value = true)
        .json(s"$root_bucket_path/raw/football_data/competition_matches/$currentDate")
    
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

  private def formattedTeams(spark:SparkSession, root_bucket_path:String, currentDate: String): Unit = {
    var teams_df = spark.read
      .option("multiline", value = true)
      .option("inferSchema", value = true)
      .json(s"$root_bucket_path/raw/football_data/teams/$currentDate.json")

    teams_df.printSchema()

    teams_df=teams_df.withColumn("team", explode(col("teams")))

    teams_df=teams_df
      .withColumn("id", col("team").getItem("id"))
      .withColumn("address", col("team").getItem("address"))
      .withColumn("name", col("team").getItem("name"))
      .withColumn("shortName", col("team").getItem("shortName"))
      .withColumn("website", col("team").getItem("website"))
      .withColumn("venue", col("team").getItem("founded"))
      .withColumn("clubColors", col("team").getItem("clubColors"))
      .withColumn("crest", col("team").getItem("crest"))
      .withColumn("tla", col("team").getItem("tla"))
      .withColumn("founded", col("team").getItem("founded"))

    teams_df = teams_df
      .drop("team")
      .drop("teams")
      .drop("count")
      .drop("filters")

    teams_df.printSchema()

    teams_df.write.mode(SaveMode.Overwrite).parquet(s"$root_bucket_path/formatted/football_data/teams/" + currentDate)
  }
}