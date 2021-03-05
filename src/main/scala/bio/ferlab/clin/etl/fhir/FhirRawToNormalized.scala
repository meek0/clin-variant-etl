package bio.ferlab.clin.etl.fhir

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import bio.ferlab.datalake.core.etl.{ETL, RawToNormalizedETL}
import org.apache.spark.sql.SparkSession

object FhirRawToNormalized extends App {

  val Array(input, output) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", value = false)
    .config("spark.delta.merge.repartitionBeforeWrite", value = true)
    .enableHiveSupport()
    .appName(s"Fhir Raw to Normalized").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  implicit val conf: Configuration = Configuration(
    List(
      StorageConf("raw", input),
      StorageConf("normalized", output)))

  val jobs: List[ETL] =
    FhirRawToNormalizedMappings
      .mappings
      .map { case (src, dst, transformations) => new RawToNormalizedETL(src, dst, transformations)}

  jobs.foreach(_.run())

}
