package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader}
import bio.ferlab.datalake.spark3.elasticsearch.{ElasticSearchClient, Indexer}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object Indexer extends App {

  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
  esNodes,          // http://0.0.0.0
  username,         // basic auth username
  password,         // basic auth password
  alias,            // alias to create
  release_id,       // release id
  templateFileName, //variants_template.json
  jobType,          //variants or genes
  lastBatch, // BAT1, BAT2 ...etc
  configFile // config/qa.conf or config/prod.conf
  ) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("es.net.http.auth.user", username)
    .config("es.net.http.auth.pass", password)
    .config("es.index.auto.create", "true")
    .config("es.net.ssl", "true")
    .config("es.net.ssl.cert.allow.self.signed", "true")
    .config("es.nodes", esNodes)
    .config("es.nodes.wan.only", "true")
    .config("es.wan.only", "true")
    .config("spark.es.nodes.wan.only", "true")
    .config("es.port", "443")
    .appName(s"Indexer")
    .getOrCreate()

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources(configFile)

  spark.sparkContext.setLogLevel("ERROR")

  val templatePath = s"${conf.storages.find(_.id == "clin_datalake").get.path}/jobs/templates/$templateFileName"

  val indexName = alias
  implicit val esClient: ElasticSearchClient = new ElasticSearchClient(esNodes.split(',').head, Some(username), Some(password))

  jobType match {
    case "variants" =>
      val job = new Indexer("index", templatePath, s"${indexName}_$release_id")
      val insertDf = VariantIndex.getInsert(Timestamp.valueOf(lastBatch))
      job.run(insertDf)

      //TODO create a column id in order to support upsert
      //val updateDf = VariantIndex.getUpdate(Timestamp.valueOf(lastBatch))
      //job.run(updateDf)

    case "genes" =>
      val job = new Indexer("index", templatePath, s"${indexName}_$release_id")

      val genesDf = conf
        .getDataset("genes")
        .read
        .select("chromosome", "symbol", "entrez_gene_id", "omim_gene_id", "hgnc",
          "ensembl_gene_id", "location", "name", "alias", "biotype")

      job.run(genesDf)
  }

}
