package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf}
import bio.ferlab.datalake.spark3.elasticsearch.{ElasticSearchClient, Indexer}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Indexer extends App {

  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
  esNodes,          // http://0.0.0.0
  username,         // basic auth username
  password,         // basic auth password
  alias,            // alias to create
  release_id,       // release id
  templateFileName, // variant_centric_template.json
  jobType,          // variants or genes
  lastBatch,        // BAT1, BAT2 ...etc
  configFile        // config/qa.conf or config/prod.conf
  ) = args

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources(configFile)
  
  val esConfigs = Map(
    "es.net.http.auth.user" -> username,
    "es.net.http.auth.pass" -> password,
    "es.index.auto.create" -> "true",
    "es.net.ssl" -> "true",
    "es.net.ssl.cert.allow.self.signed" -> "true",
    "es.nodes" -> esNodes,
    "es.nodes.wan.only" -> "true",
    "es.wan.only" -> "true",
    "spark.es.nodes.wan.only" -> "true",
    "es.port" -> "443")

  val sparkConfigs: SparkConf =
    (conf.sparkconf ++ esConfigs)
      .foldLeft(new SparkConf()){ case (c, (k, v)) => c.set(k, v) }

  implicit val spark: SparkSession = SparkSession.builder
    .config(sparkConfigs)
    .enableHiveSupport()
    .appName(s"Indexer")
    .getOrCreate()

  implicit val esClient: ElasticSearchClient = new ElasticSearchClient(esNodes.split(',').head, sanitizeArg(username), sanitizeArg(password))

  val es_index_variant_centric: DatasetConf = conf.getDataset("es_index_variant_centric")
  val ds: DatasetConf = jobType match {
    case "gene_centric" => conf.getDataset("es_index_gene_centric")
    case "gene_suggestions" => conf.getDataset("es_index_gene_suggestions")
    case "variant_centric" => conf.getDataset("es_index_variant_centric")
    case "cnv_centric" => conf.getDataset("es_index_cnv_centric")
    case "variant_suggestions" => conf.getDataset("es_index_variant_suggestions")
  }

  val df: DataFrame = spark.table(s"${ds.table.get.database}.${ds.table.get.name}_${release_id}")

  new Indexer("index", s"templates/$templateFileName", s"${alias}_$release_id")
    .run(df)

}
