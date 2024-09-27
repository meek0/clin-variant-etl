package bio.ferlab.clin.etl.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger

import java.time.LocalDateTime

abstract class Occurrences(rc: RuntimeETLContext, batchId: String) extends SimpleSingleETL(rc) {

  import spark.implicits._

  def raw_variant_calling: DatasetConf

  implicit val logger: Logger = log

  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      raw_variant_calling.id -> vcf(raw_variant_calling.location.replace("{{BATCH_ID}}", batchId), None, optional = true, split = true),
      enriched_clinical.id -> enriched_clinical.read.filter($"batch_id" === batchId)
    )
  }
}