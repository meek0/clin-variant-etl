package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.model.raw.VCF_CNV_SVClustering
import bio.ferlab.clin.etl.nextflow.NormalizeSVClustering.DataFrameOps
import bio.ferlab.clin.etl.normalized.validContigNames
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

abstract class NormalizeSVClustering(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  val source: DatasetConf
  val mainDestination: DatasetConf
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] = {
    Map(
      source.id -> vcf(source.location, None, optional = true, split = true),
      enriched_clinical.id -> enriched_clinical.read
    )
  }

  /**
   * Compute the frequency of the clusters, according to the analysis type (germline or somatic).
   *
   * @param df The normalized VCF DataFrame, exploded to have one row per cluster per sequencing.
   * @return The normalized DataFrame with frequency_RQDM column added, grouped by cluster name.
   */
  def computeFrequencyRQDM(df: DataFrame)(implicit spark: SparkSession): DataFrame

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    val inputVCF = if (data(source.id).isEmpty) {
      Seq.empty[VCF_CNV_SVClustering].toDF
    } else data(source.id).where($"contigName".isin(validContigNames: _*))

    val normalizedDf = inputVCF
      // Explode to get one row per cluster per sequencing
      .withColumn("genotype", explode($"genotypes"))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        $"genotype.sampleId" as "aliquot_id",
        $"genotype.calls" as "calls",
        $"INFO_MEMBERS" as "members",
      )
      .withClinicalInfo(data(enriched_clinical.id))

    computeFrequencyRQDM(normalizedDf)
  }
}

object NormalizeSVClustering {
  implicit class DataFrameOps(df: DataFrame) {

    def withClinicalInfo(clinical: DataFrame): DataFrame = {
      df
        .join(clinical, Seq("aliquot_id"), "left")
        .select(
          df("*"),
          clinical("affected_status")
        )
    }
  }
}
