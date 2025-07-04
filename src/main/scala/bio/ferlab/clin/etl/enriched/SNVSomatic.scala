package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.mainutils.OptionalBatch
import bio.ferlab.clin.etl.utils.ClinicalUtils.getAnalysisIdsInBatch
import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.GenomicOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locusColumnNames
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class SNVSomatic(rc: RuntimeETLContext, batchId: Option[String]) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_snv_somatic")
  val normalized_snv_somatic: DatasetConf = conf.getDataset("normalized_snv_somatic")
  val normalized_cnv_somatic_tumor_only: DatasetConf = conf.getDataset("normalized_cnv_somatic_tumor_only")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    import spark.implicits._

    batchId match {
      // If a batch id was submitted, only process analysis in the batch
      case Some(id) =>
        val clinicalDf = enriched_clinical.read
        val analysisIds: Seq[String] = getAnalysisIdsInBatch(clinicalDf, id)

        val normalizedCnvSomaticTumorOnlyDf = normalized_cnv_somatic_tumor_only.read.where($"analysis_id".isin(analysisIds: _*))
        val normalizedSnvSomaticDf = normalized_snv_somatic.read.where($"analysis_id".isin(analysisIds: _*))

        Map(
          normalized_snv_somatic.id -> normalizedSnvSomaticDf,
          normalized_cnv_somatic_tumor_only.id -> normalizedCnvSomaticTumorOnlyDf
        )
      case None =>
        // If no batch id were submitted, process all data
        Map(
          normalized_snv_somatic.id -> normalized_snv_somatic.read,
          normalized_cnv_somatic_tumor_only.id -> normalized_cnv_somatic_tumor_only.read
        )
    }
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minValue,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    val normalizedSnvSomaticDf = data(normalized_snv_somatic.id)
    val normalizedCnvSomaticTumorOnlyDf = data(normalized_cnv_somatic_tumor_only.id)

    val withCnvCount = withCount(normalizedSnvSomaticDf, normalizedCnvSomaticTumorOnlyDf, "cnv_count")

    val withAllAnalysesDf = normalizedSnvSomaticDf
      .groupByLocus($"aliquot_id")
      .agg(collect_set($"bioinfo_analysis_code") as "all_bioinfo_analysis_codes")
      .withColumn("all_analyses", when(array_contains($"all_bioinfo_analysis_codes", "TEBA"), array(lit("TO"))).otherwise(array()))
      .withColumn("all_analyses", when(array_contains($"all_bioinfo_analysis_codes", "TNEBA"), array_union($"all_analyses", array(lit("TN")))).otherwise($"all_analyses"))
      .drop("all_bioinfo_analysis_codes")

    withCnvCount
      .join(withAllAnalysesDf, locusColumnNames :+ "aliquot_id", "inner")
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("analysis_id", "bioinfo_analysis_code"), n = Some(100), sortColumns = Seq("start"))
}

object SNVSomatic {
  @main
  def run(rc: RuntimeETLContext, batch: OptionalBatch): Unit = {
    SNVSomatic(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
