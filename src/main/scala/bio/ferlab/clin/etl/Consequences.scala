package bio.ferlab.clin.etl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import columns._
import io.delta.tables.DeltaTable

object Consequences {
  val TABLE_CONSEQUENCES = "consequences"

  def run(input: String, output: String, batchId: String)(implicit spark: SparkSession): Unit = {
    val inputDF = vcf(input)
    val consequences: DataFrame = build(inputDF, batchId)
    write(consequences, output)
  }


  def build(inputDF: DataFrame, batchId: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val consequencesDF = inputDF
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        annotations
      )
      .withColumn("annotation", explode($"annotations"))
      .drop("annotations")
      .select($"*",
        consequences,
        impact,
        symbol,
        ensembl_gene_id,
        ensembl_feature_id,
        ensembl_transcript_id,
        ensembl_regulatory_id,
        feature_type,
        strand,
        biotype,
        variant_class,
        exon,
        intron,
        hgvsc,
        hgvsp,
        hgvsg,
        cds_position,
        cdna_position,
        protein_position,
        amino_acids,
        codons,
        pick,
        canonical
      )
      .drop("annotation")
      .withColumn("aa_change", when($"amino_acids".isNotNull, concat($"amino_acids.reference", $"protein_position", $"amino_acids.variant")).otherwise(lit(null)))
      .withColumn("coding_dna_change", when($"cds_position".isNotNull, concat($"cds_position", $"reference", lit(">"), $"alternate")).otherwise(lit(null)))
      .withColumn("impact_score", when($"impact" === "MODIFIER", 1).when($"impact" === "LOW", 2).when($"impact" === "MODERATE", 3).when($"impact" === "HIGH", 4).otherwise(0))
      .withColumn("consequence", explode($"consequences"))
      .withColumn("batch_id", lit(batchId))
      .drop("consequences")

    consequencesDF
  }

  private def write(consequences: DataFrame, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    if (!spark.catalog.tableExists(TABLE_CONSEQUENCES)) {
      writeOnce(consequences, output)
    } else {
      /** Merge */
      val existingConsequences = DeltaTable.forName(TABLE_CONSEQUENCES)
      existingConsequences.as("e")
        .merge(
          consequences.as("c"),
          consequences("chromosome") === $"e.chromosome" &&
            consequences("start") === $"e.start" &&
            consequences("reference") === $"e.reference" &&
            consequences("alternate") === $"e.alternate" &&
            consequences("ensembl_gene_id") === $"e.ensembl_gene_id" &&
            consequences("ensembl_feature_id") === $"e.ensembl_feature_id"
        )
        .whenNotMatched()
        .insertAll()
        .execute()

      /** Compact */
      writeOnce(spark.table(TABLE_CONSEQUENCES), output, dataChange = false)


    }
  }

  private def writeOnce(df: DataFrame, output: String, dataChange: Boolean = true)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    df.repartition(1, $"chromosome")
      .sortWithinPartitions("start")
      .write.mode(SaveMode.Overwrite)
      .option("dataChange", dataChange)
      .partitionBy("chromosome")
      .format("delta")
      .option("path", s"$output/$TABLE_CONSEQUENCES")
      .saveAsTable(TABLE_CONSEQUENCES)
  }


}
