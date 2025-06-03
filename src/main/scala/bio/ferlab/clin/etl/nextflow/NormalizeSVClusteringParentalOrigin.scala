package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.clin.etl.model.raw.VCF_CNV_SVClustering
import bio.ferlab.clin.etl.nextflow.NormalizeSVClusteringParentalOrigin.AnalysisIdExtractionRegex
import bio.ferlab.clin.etl.normalized.{Occurrences, validContigNames}
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class NormalizeSVClusteringParentalOrigin(rc: RuntimeETLContext, batchId: String) extends Occurrences(rc, batchId) {

  val raw_variant_calling: DatasetConf = conf.getDataset("nextflow_svclustering_parental_origin_output")
  override val mainDestination: DatasetConf = conf.getDataset("nextflow_svclustering_parental_origin")

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    val inputVCF = if (data(raw_variant_calling.id).isEmpty) {
      Seq.empty[VCF_CNV_SVClustering].toDF
    } else data(raw_variant_calling.id).where(col("contigName").isin(validContigNames: _*))

    val clinicalDf: DataFrame = data(enriched_clinical.id)
      .where(col("batch_id") === batchId)
      .select(
        "sequencing_id",
        "aliquot_id",
        "patient_id",
        "analysis_id",
        "gender",
        "family_id",
        "mother_id",
        "father_id",
        "affected_status"
      )

    inputVCF
      // Explode to get one row per cluster per sequencing
      .withColumn("genotype", explode(col("genotypes")))
      .select(
        chromosome,
        start,
        reference,
        alternate,
        name,
        col("genotype.sampleId") as "aliquot_id",
        col("genotype.calls") as "calls",
        col("INFO_MEMBERS") as "members",
        lit(batchId) as "batch_id",
        regexp_extract(input_file_name(), AnalysisIdExtractionRegex, 1) as "analysis_id",
        is_multi_allelic,
      )
      .join(broadcast(clinicalDf), Seq("analysis_id", "aliquot_id"), "inner")
      .withColumn("participant_id", col("patient_id"))
      .withColumn("family_info", familyInfo(Seq(col("calls"), col("affected_status"))))
      .withColumn("mother_calls", motherCalls)
      .withColumn("father_calls", fatherCalls)
      .withColumn("mother_affected_status", motherAffectedStatus)
      .withColumn("father_affected_status", fatherAffectedStatus)
      .withColumn("zygosity", zygosity(col("calls")))
      .withParentalOrigin("parental_origin", col("calls"), col("father_calls"), col("mother_calls"))
      .withGenotypeTransmission("transmission")
      .drop("family_info", "participant_id")
  }
}

object NormalizeSVClusteringParentalOrigin {
  final val AnalysisIdExtractionRegex = "([A-Z]*\\d+)\\.[^\\.]+\\.(?:DEL|DUP)\\.vcf\\.gz"

  @main
  def run(rc: RuntimeETLContext, batch: Batch): Unit = {
    NormalizeSVClusteringParentalOrigin(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
