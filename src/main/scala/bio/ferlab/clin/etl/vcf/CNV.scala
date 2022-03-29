package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.vcf.CNV.getCNV
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class CNV(batchId: String)(implicit configuration: Configuration) extends Occurrences(batchId) {

  override val destination: DatasetConf = conf.getDataset("normalized_cnv")
  override val raw_variant_calling: DatasetConf = conf.getDataset("raw_cnv")

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val joinedRelation: DataFrame = getClinicalRelation(data)

    val occurrences = getCNV(data(raw_variant_calling.id), batchId)
      .join(joinedRelation, Seq("aliquot_id"), "inner")
    occurrences
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(10, col("patient_id"))
    )
  }
}

object CNV {
  def getCNV(inputDf: DataFrame, batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val df =
      inputDf
        .withColumn("genotype", explode(col("genotypes"))).select(
        chromosome,
        start,
        reference,
        alternate,
        name,
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
        $"INFO_END" as "end",
        $"INFO_SVTYPE" as "svtype",
        flatten(transform($"INFO_FILTERS", c => split(c, ";"))) as "filters")
        df
  }
}


