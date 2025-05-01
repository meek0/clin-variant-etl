package bio.ferlab.clin.etl.nextflow

import bio.ferlab.clin.etl.model.raw.VCF_CNV_SVClustering
import bio.ferlab.clin.etl.normalized.validContigNames
import bio.ferlab.clin.etl.utils.FrequencyUtils.pcNoFilter
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class NormalizeSVClustering(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  val raw_variant_calling: DatasetConf = conf.getDataset("nextflow_svclustering_output")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  override val mainDestination: DatasetConf = conf.getDataset("nextflow_svclustering")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] = {
    Map(
      raw_variant_calling.id -> vcf(raw_variant_calling.location, None, optional = true, split = true),
      enriched_clinical.id -> enriched_clinical.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    val inputVCF = if (data(raw_variant_calling.id).isEmpty) {
      Seq.empty[VCF_CNV_SVClustering].toDF
    } else data(raw_variant_calling.id).where(col("contigName").isin(validContigNames: _*))

    val normalizedDf = inputVCF
      // Explode to get one row per cluster per sequencing
      .withColumn("genotype", explode(col("genotypes")))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        col("genotype.sampleId") as "aliquot_id",
        col("genotype.calls") as "calls",
        col("INFO_MEMBERS") as "members",
      )

    // Count total number of aliquot_ids in svclustering result
    val pn: Long = normalizedDf.select($"aliquot_id").distinct().count()

    normalizedDf
      .groupBy($"name")
      .agg(
        first("members") as "members",
        first($"chromosome") as "chromosome",
        first($"start") as "start",
        first($"end") as "end",
        first($"alternate") as "alternate",
        first($"reference") as "reference",
        struct(
          lit(pn) as "pn",
          pcNoFilter,
        ) as "frequency_RQDM"
      )
      .withColumn("frequency_RQDM", $"frequency_RQDM".withField("pf",
        coalesce($"frequency_RQDM.pc" / $"frequency_RQDM.pn", lit(0.0))))
  }
}

object NormalizeSVClustering {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    NormalizeSVClustering(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
