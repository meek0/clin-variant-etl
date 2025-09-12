package bio.ferlab.clin.etl.script

import bio.ferlab.clin.etl.script.schema.SchemaUtils.runCompactFor
import bio.ferlab.datalake.commons.config.{Configuration, RuntimeETLContext}
import org.apache.spark.sql.SparkSession

case class Compact(rc: RuntimeETLContext) {
  implicit val spark: SparkSession = rc.spark
  implicit val conf: Configuration = rc.config

  def run(datasetIds: Seq[String]): Unit = {
    runCompactFor(datasetIds)
  }
}