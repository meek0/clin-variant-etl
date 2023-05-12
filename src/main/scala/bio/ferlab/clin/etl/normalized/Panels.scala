package bio.ferlab.clin.etl.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.utils.FixedRepartition
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.LocalDateTime

class Panels()(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_panels")
  val raw_panels: DatasetConf = conf.getDataset("raw_panels")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_panels.id -> raw_panels.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
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

