package bio.ferlab.clin.etl.script

import bio.ferlab.clin.etl.script.schema.SchemaUtils.runVacuumFor
import bio.ferlab.datalake.commons.config.{Configuration, RuntimeETLContext}
import org.apache.spark.sql.SparkSession

case class Vacuum(rc: RuntimeETLContext) {
  implicit val spark: SparkSession = rc.spark
  implicit val conf: Configuration = rc.config

  def run(datasetIds: Seq[String], nbVersions: Int): Unit = {
    runVacuumFor(datasetIds, nbVersions)
  }
}