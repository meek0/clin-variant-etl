package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.SNV.transformSingleSNV
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RepartitionByColumns}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.GenomicOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumnNames}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class SNVSomaticTumorOnly()(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_snv_somatic_tumor_only")
  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv_somatic_tumor_only")
  val normalized_exomiser: DatasetConf = conf.getDataset("normalized_exomiser")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_snv.id -> normalized_snv.read,
      normalized_exomiser.id -> normalized_exomiser.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    transformSingleSNV(data(normalized_snv.id), data(normalized_exomiser.id))
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(1), sortColumns = Seq("start"))
}
