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
      .withColumn("consequence", explode($"consequences"))
      .withColumn("batch_id", lit(batchId))
      .drop(TABLE_CONSEQUENCES)

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
            consequences("ensembl_transcript_id") === $"e.ensembl_transcript_id" &&
            consequences("ensembl_regulatory_id") === $"e.ensembl_regulatory_id"
        )
        .whenNotMatched()
        .insertAll()
        .execute()

      /** Compact */
      writeOnce(spark.table(TABLE_CONSEQUENCES), output)


    }
  }

  private def writeOnce(df: DataFrame, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    df.repartition(1, $"chromosome")
      .sortWithinPartitions("start")
      .write.mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("delta")
      .option("path", s"$output/$TABLE_CONSEQUENCES")
      .saveAsTable(TABLE_CONSEQUENCES)
  }


}
