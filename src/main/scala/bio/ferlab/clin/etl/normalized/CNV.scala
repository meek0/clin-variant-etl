package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.normalized.CNV.getCNV
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.utils.RepartitionByColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime

class CNV(batchId: String)(implicit configuration: Configuration) extends Occurrences(batchId) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_cnv")
  override val raw_variant_calling: DatasetConf = conf.getDataset("raw_cnv")

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val joinedRelation: DataFrame = getClinicalRelation(data)

    val occurrences = getCNV(data(raw_variant_calling.id), batchId)
      .join(broadcast(joinedRelation), Seq("aliquot_id"), "inner")
    occurrences
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(Seq("patient_id"), Some(10))

  override def replaceWhere: Option[String] = Some(s"batch_id = '$batchId'")

}

object CNV {

  def getCNV(inputDf: DataFrame, batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val df =
      inputDf
        .withColumn("genotype", explode(col("genotypes")))
        .select(
          chromosome,
          start,
          reference,
          alternate,
          name,
          $"qual",
          $"genotype.sampleId" as "aliquot_id",
          $"genotype.BC" as "bc",
          $"genotype.SM" as "sm",
          $"genotype.calls" as "calls",
          $"genotype.CN" as "cn",
          $"genotype.pe" as "pe",
          is_multi_allelic,
          old_multi_allelic,
          $"INFO_CIEND" as "ciend",
          $"INFO_CIPOS" as "cipos",
          $"INFO_SVLEN"(0) as "svlen",
          $"INFO_REFLEN" as "reflen",
          $"start" + $"INFO_REFLEN" as "end",
          $"INFO_SVTYPE" as "svtype",
          flatten(transform($"INFO_FILTERS", c => split(c, ";"))) as "filters",
          lit(batchId) as "batch_id")
        .withColumn("type", split(col("name"), ":")(1))
        .withColumn("sort_chromosome", sortChromosome)
    df
  }
}
