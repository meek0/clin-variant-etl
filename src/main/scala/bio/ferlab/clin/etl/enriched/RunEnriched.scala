package bio.ferlab.clin.etl.enriched

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.util.Try

object RunEnriched extends App {

  val Array(input, output, lastExecutionString, runType) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", value = false)
    .config("spark.delta.merge.repartitionBeforeWrite", value = true)
    //    .config("", value = 20) //Avoid too many small files as output of merging delta
    .enableHiveSupport()
    .appName(s"Run Enriched $runType").getOrCreate()
  val minimumDateTime = LocalDateTime.of(1900, 1 , 1, 0, 0, 0)
  val lastExecutionTimestamp = Try(Timestamp.valueOf(lastExecutionString)).getOrElse(Timestamp.valueOf(minimumDateTime))

  run(input, output, lastExecutionTimestamp, runType)

  def run(input: String, output: String, lastExecutionTimestamp: Timestamp, runType: String = "all")(implicit spark: SparkSession): Unit = {
    runType match {
      case "variants" => Variants.run(input, output, lastExecutionTimestamp)
      case "consequences" => Consequences.run(input, output, lastExecutionTimestamp)
      case "all" =>
        Variants.run(input, output, lastExecutionTimestamp)
        Consequences.run(input, output, lastExecutionTimestamp)
      case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
    }

  }


}

