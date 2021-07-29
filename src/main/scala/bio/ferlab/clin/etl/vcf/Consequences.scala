package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.utils.VcfUtils.columns._
import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Consequences(batchId: String)(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("normalized_consequences")
  val complete_joint_calling: DatasetConf = conf.getDataset("complete_joint_calling")

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      //TODO add vcf normalization
      complete_joint_calling.id -> vcf(complete_joint_calling.location, referenceGenomePath = None)
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(complete_joint_calling.id)
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
      .withColumn("createdOn", lit(batchId))//current_timestamp())
      .withColumn("updatedOn", lit(batchId))//current_timestamp())
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions("start"))
  }
}
