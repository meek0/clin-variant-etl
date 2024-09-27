package bio.ferlab.clin.etl.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, FixedRepartition, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, functions}

import java.time.LocalDateTime

case class Panels(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_panels")
  val raw_panels: DatasetConf = conf.getDataset("raw_panels")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      raw_panels.id -> raw_panels.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minValue,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    data(raw_panels.id)
      .select(
        $"symbol",
        functions.split(col("panels"), ",").as("panels"),
        functions.split(col("version"), ",").as("version"),
      )
  }

  override def defaultRepartition: DataFrame => DataFrame = FixedRepartition(1)

}

object Panels {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    Panels(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
