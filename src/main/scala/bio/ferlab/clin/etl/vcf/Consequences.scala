package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.utils.DeltaUtils
import bio.ferlab.clin.etl.utils.VcfUtils._
import bio.ferlab.clin.etl.utils.VcfUtils.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Consequences {
  val TABLE_CONSEQUENCES = "consequences"

  def run(input: String, output: String, batchId: String)(implicit spark: SparkSession): Unit = {
    val inputDF = vcf(input)
    val consequences: DataFrame = build(inputDF, batchId)

    DeltaUtils.upsert(
      consequences,
      Some(output),
      "clin_raw",
      TABLE_CONSEQUENCES,
      {
        _.repartition(1, col("chromosome")).sortWithinPartitions("start")
      },
      locusColumnNames :+ "ensembl_gene_id" :+ "ensembl_feature_id",
      Seq("chromosome"))
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
      .withColumn("batch_id", lit(batchId))
      .withColumn("createdOn", current_timestamp())
      .withColumn("updatedOn", current_timestamp())

    consequencesDF
  }
}
