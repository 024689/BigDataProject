error id: file://<WORKSPACE>/src/main/scala/RawToFormatted.scala:[3049..3050) in Input.VirtualFile("file://<WORKSPACE>/src/main/scala/RawToFormatted.scala", "import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate

object RawToFormatted {
  def main(args: Array[String]): Unit = {
    val GCS_JSON_key = "<HOME>/Documents/projects/BigData/DataLake/utils/credentials/google_storage_secret_key_credentials.json"

    val spark = SparkSession.builder
      .appName("RawToFormatted")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration
      .set("google.cloud.auth.service.account.json.keyfile", GCS_JSON_key)

    if (args.length > 0) {
      val ROOT_BUCKET_PATH = args(0)
      this.formatCompetition(spark, ROOT_BUCKET_PATH)
    } else {
      throw new RuntimeException("Provide arguments for google cloud bucket path")
    }
  }

  private def formatCompetition(spark: SparkSession, root_bucket_path: String): Unit = {

    val currentDate = LocalDate.now()
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

  private def 
}
")
file://<WORKSPACE>/src/main/scala/RawToFormatted.scala
file://<WORKSPACE>/src/main/scala/RawToFormatted.scala:73: error: expected identifier; obtained rbrace
}
^
#### Short summary: 

expected identifier; obtained rbrace