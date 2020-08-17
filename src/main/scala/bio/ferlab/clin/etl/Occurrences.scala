package bio.ferlab.clin.etl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import columns._
import io.delta.tables.DeltaTable

object Occurrences {

  def run(input: String, output: String, batchId: String)(implicit spark: SparkSession): Unit = {
    write(build(input, batchId), output)
  }

  def build(input: String, batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val occurrences = vcf(input)
      .withColumn("genotype", explode($"genotypes"))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        firstAnn,
        $"genotype.sampleId" as "biospecimen_id",
        $"genotype.alleleDepths" as "ad",
        $"genotype.depth" as "dp",
        $"genotype.conditionalQuality" as "gq",
        $"genotype.calls" as "calls",
        array_contains($"genotype.calls", 1) as "has_alt",
        is_multi_allelic,
        old_multi_allelic
      )
      .withColumn("zygosity", zygosity)
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .withColumn("batch_id", lit(batchId))
      .drop("annotation")
      .where($"chromosome" === "X")
    //
    //    val biospecimens = broadcast(
    //      spark
    //        .table("biospecimens")
    //        .select($"biospecimen_id", $"patient_id", $"family_id")
    //    )
    //
    //    occurrences
    //      .join(biospecimens, occurrences("biospecimen_id") === biospecimens("biospecimen_id"), "inner")
    //      .drop(occurrences("biospecimen_id"))
    occurrences
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
