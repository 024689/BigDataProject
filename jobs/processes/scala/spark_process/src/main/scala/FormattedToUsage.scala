import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, countDistinct, sum}
import org.elasticsearch.spark.sql._

import java.time.LocalDate

object FormattedToUsage {

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

//    val executionDate = spark.conf.get("spark.airflow.execution_date")
//    println("************************************************")
//    println(s"Date d'exécution  : $executionDate")
//    println("************************************************")


      val ROOT_BUCKET_PATH = args(0)
//      val currentDate: String = if(executionDate != null) executionDate else LocalDate.now().toString
      val currentDate: String = "2024-01-14"
      this.combineCompetitions(spark, ROOT_BUCKET_PATH, currentDate)
    } else {
      throw new RuntimeException("Provide arguments for google cloud bucket path")
    }
  }

  private def combineCompetitions(spark: SparkSession, ROOT_BUCKET_PATH: String, currentDate: String): Unit ={
    var season_df = spark.read.option("inferSchema", value = true).parquet(s"$ROOT_BUCKET_PATH/formatted/football_data/season/$currentDate")
    var matches_df = spark.read.option("inferSchema", value = true)
      .parquet(s"$ROOT_BUCKET_PATH/formatted/football_data/matches/$currentDate")
    var competitions_df = spark.read.option("inferSchema", value = true).parquet(s"$ROOT_BUCKET_PATH/formatted/football_data/competition/$currentDate")

    competitions_df = competitions_df.drop("competitionId")
    matches_df = matches_df.withColumnRenamed("id", "pkMatchId")
    matches_df = matches_df.withColumnRenamed("matchId", "pkMatchId")
    season_df = season_df.withColumnRenamed("id", "pkSeason")
    competitions_df = competitions_df.withColumnRenamed("id","pkCompetition")

    val combine_MC_df = matches_df.join(competitions_df).where(matches_df("competitionId") === competitions_df("pkCompetition"))
    combine_MC_df.printSchema()

    var combine_MCS_df = combine_MC_df
      .join(season_df)
      .where(combine_MC_df("seasonId") === season_df("pkSeason"))
    combine_MCS_df.printSchema()

    combine_MCS_df = combine_MCS_df.drop("competitionId")

    val competition_res_df = combine_MCS_df
      .groupBy(col("pkCompetition") as "competitionId", col("name") as "competitionName")
      .agg(count(col("pkMatchId")) as "totalMatch",
        sum(col("scoreFullTime").getItem("home") + col("scoreFullTime").getItem("away")) as "totalFullTimeScore",
        sum(col("scoreHalfTime").getItem("home") + col("scoreHalfTime").getItem("away")) as "totalHalfTimeScore")

    competition_res_df.show()

    val comb2 = combine_MCS_df.join(competition_res_df)
      .where(combine_MCS_df("pkCompetition") === competition_res_df("competitionId"))

    val competitions_result_df = comb2
      .groupBy(col("pkCompetition") as "competitionId",
        col("name"), col("totalHalfTimeScore"),
        col("totalFullTimeScore"), col("totalMatch"))
      .agg(countDistinct(col("awayTeamId")) as "totalTeams")

    competitions_result_df.show()

    competitions_result_df.write.mode(SaveMode.Overwrite).parquet(s"$ROOT_BUCKET_PATH/usage/football_data/competitions/" + currentDate)

    competitions_result_df.write
      .format("org.elasticsearch.spark.sql")
      .option("es.resource", "competition/comp")
      .option("es.nodes.wan.only", value = true)
      .mode(SaveMode.Append)
      .save()
  }
}
