package bio.ferlab.clin.etl

import bio.ferlab.clin.etl.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Variants {

  def run(input: String, output: String, batchId: String)(implicit spark: SparkSession): Unit = {
    val inputDF = vcf(input)
    val annotations: DataFrame = build(inputDF, batchId)

    SparkUtils.upsert(
      annotations,
      Some(output),
      "clin_raw",
      "variants",
      {_.repartition(1, col("chromosome")).sortWithinPartitions("start")},
      locusColumnNames,
      Seq("chromosome"))

    //write(annotations, output)
    //    val deltaTable = DeltaTable.forName("variants")
    //    deltaTable.vacuum()

  }

  def build(inputDF: DataFrame, batchId: String)(implicit spark: SparkSession): DataFrame = {
    val variants = inputDF
      .withColumn("annotation", firstAnn)
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        is_multi_allelic,
        old_multi_allelic,
        array_distinct(annotations("symbol")) as "genes_symbol",
        hgvsg,
        variant_class,
        pubmed,
        lit(batchId) as "batch_id",
        lit(null).cast("string") as "last_batch_id",
        current_timestamp() as "createdOn",
        current_timestamp() as "updatedOn"
      )
      .drop("annotation")

    variants
  }
}
