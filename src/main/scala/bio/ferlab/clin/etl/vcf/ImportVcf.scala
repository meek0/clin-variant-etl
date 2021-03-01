package bio.ferlab.clin.etl.vcf

import org.apache.spark.sql.SparkSession

object ImportVcf extends App {

  val Array(input, output, batchId, runType) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", value = false)
    .config("spark.delta.merge.repartitionBeforeWrite", value = true)
    //    .config("", value = 20) //Avoid too many small files as output of merging delta
    .enableHiveSupport()
    .appName(s"Import $runType").getOrCreate()

  run(input, output, batchId, runType)

  def run(input: String, output: String, batchId: String, runType: String = "all")(implicit spark: SparkSession): Unit = {
    spark.sql("use clin_raw")

    runType match {
      case "variants" => Variants.run(input, output, batchId)
      case "consequences" => Consequences.run(input, output, batchId)
      case "occurrences" => Occurrences.run(input, output, batchId)
      case "all" =>
        Occurrences.run(input, output, batchId)
        Variants.run(input, output, batchId)
        Consequences.run(input, output, batchId)
      case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
    }


  }
}
