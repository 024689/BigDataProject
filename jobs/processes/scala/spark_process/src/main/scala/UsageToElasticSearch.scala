import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

object UsageToElasticSearch {

  def main(args: Array[String]): Unit = {

    val esNodeHost = sys.env("ES_NODE_HOST")
    val GCS_JSON_key = "../../../../../../../utils/credentials/google_storage_secret_key_credentials.json"
    if (args.length > 1) {
      val esResource = args(1)
      val sourceDataPath = args(0)
      val options = Map(
        "es.nodes" -> esNodeHost,
        "es.port" -> "9200",
        "es.resource" -> esResource,
        "es.nodes.client.only" -> true,
      )

      val spark = SparkSession.builder
        .appName("ParquetToElasticsearch")
        .config(options)
        .getOrCreate()

      spark.sparkContext.hadoopConfiguration
        .set("google.cloud.auth.service.account.json.keyfile", GCS_JSON_key)

      val parquetData = spark.read.parquet(sourceDataPath)
      parquetData.saveToEs(esResource)

      print(f"**** DATA SUCCESSFULLY SAVED IN ELASTIC_SEARCH ****")
      spark.stop()
    }
  }
}
