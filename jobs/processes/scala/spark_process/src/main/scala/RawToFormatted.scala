import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object RawToFormatted {
  def main(args: Array[String]): Unit = {
    val GCS_JSON_key = "../../../../../../../utils/credentials/google_storage_secret_key_credentials.json"

    val spark = SparkSession.builder
      .appName("ParquetToElasticsearch")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration
      .set("google.cloud.auth.service.account.json.keyfile", GCS_JSON_key)

    if(args.length > 0) {
      val ROOT_BUCKET_PATH = args(0)

      var competition_df = spark.read.option("multiline", value = true)
        .option("inferSchema", value = true)
        .json(s"$ROOT_BUCKET_PATH/raw/football_data/competitions/2024-01-14/*.json")

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
    } else {
      throw new RuntimeException("Provide arguments for google cloud bucket path")
    }

  }
}
