package bio.ferlab.clin.etl.migration

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.SparkApp
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.utils.RepartitionByRange
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
/**
 * This app copy dbnsfp tables into new directories, accordingly to public dataset configuration in datalake lib:
 *
 */
object ConvertDBNSFPTablesToDelta extends SparkApp {

  implicit val (conf, _, spark) = init(appName = s"Convert Public tables to parquet")

  new ConvertDBNSFPTablesToDelta().run()

}

class ConvertDBNSFPTablesToDelta()(implicit conf: Configuration) extends ETL {
  override def mainDestination: DatasetConf = conf.getDataset("enriched_dbnsfp")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = Map(
    "normalized_dbnsfp" -> conf.getDataset("deprecated_normalized_dbnsfp_scores").read,
    "enriched_dbnsfp" -> conf.getDataset("deprecated_normalized_dbnsfp_original").read,
  )

  override def transform(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = data

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1000))
}
