package bio.ferlab.clin.etl.migration

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.SparkApp
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.utils.RepartitionByRange
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
/**
 * This app copy all gnomad tables into new directories, accordingly to public dataset configuration in datalake lib:
 * - normalized_gnomad_genomes_2_1_1 is copy to normalized_gnomad_genomes_v2_1_1
 * - normalized_gnomad_exomes_2_1_1 is copy to normalized_gnomad_exomes_v2_1_1
 * - normalized_gnomad_genomes_3_1_1 is copy to normalized_gnomad_genomes_v3
 * - normalized_gnomad_genomes_3_0 is not kept because in the future we'll keep only one version of gnomad per major
 *
 */
object ConvertGnomadTablesToDelta extends SparkApp {

  implicit val (conf, _, spark) = init(appName = s"Convert Public tables to parquet")

  new ConvertGnomadTablesToDelta().run()

}

class ConvertGnomadTablesToDelta()(implicit conf: Configuration) extends ETL {
  override def mainDestination: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v3")

  override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = Map(
    "normalized_gnomad_genomes_v2_1_1" -> conf.getDataset("normalized_gnomad_genomes_2_1_1").read,
    "normalized_gnomad_exomes_v2_1_1" -> conf.getDataset("normalized_gnomad_exomes_2_1_1").read,
    "normalized_gnomad_genomes_v3" -> conf.getDataset("normalized_gnomad_genomes_3_1_1").read,
  )

  override def transform(data: Map[String, DataFrame], lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = data

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1000))
}
