package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.utils.DeltaUtils
import bio.ferlab.clin.etl.utils.VcfUtils.columns._
import bio.ferlab.clin.etl.utils.VcfUtils.vcf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Occurrences {

  val OCCURRENCES_TABLE = "occurrences"

  def run(input: String, output: String, batchId: String)(implicit spark: SparkSession): Unit = {
    val occurrences = vcf(input)
    val updates = build(occurrences, batchId)
    DeltaUtils.insert(
      updates,
      Some(output),
      "clin_raw",
      OCCURRENCES_TABLE,
      {
        _.repartition(1, col("chromosome"))
          .sortWithinPartitions(col("chromosome"), col("start"))
      },
      Seq("chromosome")
    )
  }

  def build(inputDf: DataFrame, batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val occurrences = inputDf
      .withColumn("genotype", explode($"genotypes"))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        firstAnn,
        concat(lit("SP"), $"genotype.sampleId") as "biospecimen_id",
        $"genotype.alleleDepths" as "ad",
        $"genotype.depth" as "dp",
        $"genotype.conditionalQuality" as "gq",
        $"genotype.calls" as "calls",
        $"INFO_QD" as "qd",
        array_contains($"genotype.calls", 1) as "has_alt",
        is_multi_allelic,
        old_multi_allelic
      )
      .withColumn("ad_ref", $"ad"(0))
      .withColumn("ad_alt", $"ad"(1))
      .withColumn("ad_total", $"ad_ref" + $"ad_alt")
      .withColumn("ad_ratio", when($"ad_total" === 0, 0).otherwise($"ad_alt" / $"ad_total"))
      .drop("ad")
      .withColumn("zygosity", zygosity)
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_update", current_date())
      .withColumn("variant_type", lit("germline"))
      .drop("annotation")

    val patients = spark.table("patients")
    val biospecimens = spark.table("biospecimens")

    val biospecimensWithPatient = broadcast(
      biospecimens
        .join(patients, Seq("patient_id"))
        .select($"biospecimen_id", $"patient_id", $"family_id", $"practitioner_id", $"organization_id", $"sequencing_strategy", $"study_id")
    )

    occurrences.join(biospecimensWithPatient, Seq("biospecimen_id"), "inner")

  }
}
