package bio.ferlab.clin.etl.enriched

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.util.Try

object RunEnriched extends App {

  val Array(input, output, lastExecutionDateTime, runType) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", value = false)
    .config("spark.delta.merge.repartitionBeforeWrite", value = true)
    //    .config("", value = 20) //Avoid too many small files as output of merging delta
    .enableHiveSupport()
    .appName(s"Run Enriched $runType").getOrCreate()
  val minimumDateTime = LocalDateTime.of(1900, 1 , 1, 0, 0, 0)

  run(input, output, Try(Timestamp.valueOf(lastExecutionDateTime)).getOrElse(Timestamp.valueOf(minimumDateTime)), runType)

  def run(input: String, output: String, lastExecutionDateTime: Timestamp, runType: String = "all")(implicit spark: SparkSession): Unit = {
    runType match {
      case "variants" => Variants.run(input, output, lastExecutionDateTime)
      case "consequences" => Consequences.run(input, output, lastExecutionDateTime)
      case "all" =>
        Variants.run(input, output, lastExecutionDateTime)
        Consequences.run(input, output, lastExecutionDateTime)
      case s: String => throw new IllegalArgumentException(s"Runtype [$s] unknown.")
    }

  }


}

