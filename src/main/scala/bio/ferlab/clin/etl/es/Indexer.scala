package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.spark3.elasticsearch.{ElasticSearchClient, Indexer}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object Indexer extends App {

  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
  input,
  esNodes,
  alias,
  oldRelease,
  newRelease,
  templateFileName,
  jobType,
  batchSize,
  _,
  format,
  _
  ) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("es.index.auto.create", "true")
    .config("es.nodes", esNodes)
    .config("es.batch.size.entries", batchSize)
    .appName(s"Indexer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val templatePath = s"s3://clin/jobs/templates/$templateFileName"

  val indexName = alias

  println(s"$jobType - ${indexName}_$newRelease")

  val job = new Indexer(jobType, templatePath, alias, s"${indexName}_$newRelease", Some(s"${indexName}_$oldRelease"))
  implicit val esClient: ElasticSearchClient = new ElasticSearchClient(esNodes.split(',').head)

  val df: DataFrame = spark.read.format(format).load(input)

  job.run(df)(esClient)
}
