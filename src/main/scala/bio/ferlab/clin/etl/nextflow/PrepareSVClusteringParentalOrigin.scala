package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.clin.etl.utils.ClinicalUtils.getAnalysisIdsInBatch
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.utils.CsvUtils
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.regexp_replace

import java.time.LocalDateTime

case class PrepareSVClusteringParentalOrigin(rc: RuntimeETLContext, batchId: String) extends SimpleSingleETL(rc) {

  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  override val mainDestination: DatasetConf = conf.getDataset("nextflow_svclustering_parental_origin_input")
    .replacePath("{{BATCH_ID}}", batchId)

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] = {
    Map(enriched_clinical.id -> enriched_clinical.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    val clinicalDf = data(enriched_clinical.id)
    val analysesInCurrentBatch: Seq[String] = getAnalysisIdsInBatch(clinicalDf, batchId)

    val analysesWithAtLeastOneParent = clinicalDf
      .where($"mother_aliquot_id".isNotNull or $"father_aliquot_id".isNotNull)
      .select("analysis_id")
      .distinct()

    clinicalDf
      .where($"analysis_id".isin(analysesInCurrentBatch: _*))
      .where($"cnv_vcf_germline_urls".isNotNull)
      .join(analysesWithAtLeastOneParent, Seq("analysis_id"), "inner")
      .select(
        $"aliquot_id" as "sample",
        $"analysis_id" as "familyId",
        regexp_replace($"cnv_vcf_germline_urls"(0), "s3a://", "s3://") as "vcf" // There's always a single file
      )
      .distinct()
  }

  override def loadSingle(data: DataFrame,
                          lastRunValue: LocalDateTime,
                          currentRunValue: LocalDateTime): DataFrame = {
    if (!data.isEmpty) {
      super.loadDataset(data, mainDestination)
      CsvUtils.renameCsvFile(mainDestination)
    } else {
      log.warn("No CNV files for all analyses in batch. No CSV file to output.")
      data
    }
  }
}

object PrepareSVClusteringParentalOrigin {
  @main
  def run(rc: RuntimeETLContext, batch: Batch): Unit = {
    PrepareSVClusteringParentalOrigin(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
