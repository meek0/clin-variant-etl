package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.clin.etl.model.raw.VCF_CNV_Input
import bio.ferlab.clin.etl.normalized.CNV.getCNV
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

case class CNV(rc: RuntimeETLContext, batchId: String) extends Occurrences(rc, batchId) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_cnv")
  override val raw_variant_calling: DatasetConf = conf.getDataset("raw_cnv")

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minValue,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {

    import spark.implicits._

    val inputVCF = if (data(raw_variant_calling.id).isEmpty) Seq.empty[VCF_CNV_Input].toDF else data(raw_variant_calling.id).where(col("contigName").isin(validContigNames: _*))

    val clinicalDf: DataFrame = data(enriched_clinical.id)
      .where($"batch_id" === batchId)
      .drop("batch_id")

    val occurrences = getCNV(inputVCF, batchId)
      .join(broadcast(clinicalDf), Seq("aliquot_id"), "inner")

    occurrences
  }

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
        .withColumn("variant_type", lit("germline"))
    df
  }

  @main
  def run(rc: RuntimeETLContext, batch: Batch): Unit = {
    CNV(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
