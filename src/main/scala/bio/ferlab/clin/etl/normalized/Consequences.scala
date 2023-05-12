package bio.ferlab.clin.etl.normalized


import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.etl.v2.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import bio.ferlab.datalake.spark3.utils.DeltaUtils.{compact, vacuum}
import bio.ferlab.datalake.spark3.utils.RepartitionByColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

class Consequences(batchId: String)(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_consequences")
  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_variant_calling.id ->
        vcf(raw_variant_calling.location.replace("{{BATCH_ID}}", batchId), referenceGenomePath = None)
          .where(col("contigName").isin(validContigNames: _*))
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val df = data(raw_variant_calling.id)
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        csq
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
        regexp_replace(hgvsp, "%3D", "=") as "hgvsp",
        hgvsg,
        cds_position,
        cdna_position,
        protein_position,
        amino_acids,
        codons,
        original_canonical,
        array_distinct(split(col("annotation.RefSeq"), "&")) as "refseq_mrna_id"
      )
      .drop("annotation")
      .withColumn("aa_change",
        concat(
          lit("p."),
          normalizeAminoAcid($"amino_acids.reference"),
          $"protein_position",
          when($"amino_acids.variant".isNull, lit("=")).otherwise(normalizeAminoAcid($"amino_acids.variant"))))
      .withColumn("coding_dna_change", when($"cds_position".isNotNull, concat(lit("c."), $"cds_position", $"reference", lit(">"), $"alternate")).otherwise(lit(null)))
      .withColumn("impact_score", when($"impact" === "MODIFIER", 1).when($"impact" === "LOW", 2).when($"impact" === "MODERATE", 3).when($"impact" === "HIGH", 4).otherwise(0))
      .withColumn("batch_id", lit(batchId))
      .withColumn("created_on", lit(Timestamp.valueOf(currentRunDateTime)))
      .withColumn("updated_on", lit(Timestamp.valueOf(currentRunDateTime)))
      .withColumn(mainDestination.oid, col("created_on"))
      .dropDuplicates("chromosome", "start", "reference", "alternate", "ensembl_transcript_id")
    df
  }

  def normalizeAminoAcid(amino_acid: Column): Column = {
    val aminoAcidMap =
      Map(
        "A" -> "Ala",
        "R" -> "Arg",
        "N" -> "Asn",
        "D" -> "Asp",
        "B" -> "Asx",
        "C" -> "Cys",
        "E" -> "Glu",
        "Q" -> "Gln",
        "Z" -> "Glx",
        "G" -> "Gly",
        "H" -> "His",
        "I" -> "Ile",
        "L" -> "Leu",
        "K" -> "Lys",
        "M" -> "Met",
        "F" -> "Phe",
        "P" -> "Pro",
        "S" -> "Ser",
        "T" -> "Thr",
        "W" -> "Trp",
        "Y" -> "Tyr",
        "V" -> "Val",
        "X" -> "Xaa",
        "*" -> "Ter"
      )
    aminoAcidMap
      .tail
      .foldLeft(when(amino_acid === aminoAcidMap.head._1, lit(aminoAcidMap.head._2))) { case (c, (a, aaa)) =>
        c.when(amino_acid === a, lit(aaa))
      }.otherwise(amino_acid)
  }


  override def publish()(implicit spark: SparkSession): Unit = {
    compact(mainDestination, RepartitionByColumns(Seq("chromosome"), Some(10), Seq(col("start"))))
    vacuum(mainDestination, 2)
  }

}
