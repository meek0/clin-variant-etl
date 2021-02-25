package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Occurrences {

  def run(input: String, output: String, batchId: String)(implicit spark: SparkSession): Unit = {
    val occurrences = vcf(input)
    write(build(occurrences, batchId), output)
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

  val OCCURRENCES_TABLE = "occurrences"

  def write(occ: DataFrame, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    occ
      .repartition(1, $"chromosome")
      .sortWithinPartitions($"chromosome", $"start")
      .write.mode(SaveMode.Append)
      .partitionBy("chromosome")
      .format("delta")
      .option("path", s"$output/$OCCURRENCES_TABLE")
      .saveAsTable(OCCURRENCES_TABLE)

    //Compact
    spark.table(OCCURRENCES_TABLE)
      .repartition(1, $"chromosome")
      .sortWithinPartitions($"chromosome", $"start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("delta")
      .option("path", s"$output/$OCCURRENCES_TABLE")
      .saveAsTable(OCCURRENCES_TABLE)

  }
}
