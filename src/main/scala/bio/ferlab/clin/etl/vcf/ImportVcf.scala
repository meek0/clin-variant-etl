package bio.ferlab.clin.etl.vcf

import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader}
import org.apache.spark.sql.SparkSession

object ImportVcf extends App {

  val Array(batchId, runType, config) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", value = false)
    .config("spark.delta.merge.repartitionBeforeWrite", value = true)
    //    .config("", value = 20) //Avoid too many small files as output of merging delta
    .enableHiveSupport()
    .appName(s"Import $runType").getOrCreate()

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources(config)

  runType match {
    case "variants" => new Variants(batchId).run()
    case "consequences" => new Consequences(batchId).run()
    case "occurrences" => new Occurrences(batchId).run()
    case "all" =>
      new Occurrences(batchId).run()
      new Variants(batchId).run()
      new Consequences(batchId).run()
    case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
  }
}
