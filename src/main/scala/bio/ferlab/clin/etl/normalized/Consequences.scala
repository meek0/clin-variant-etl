package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.utils.DeltaUtils.{compact, vacuum}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

import java.sql.Timestamp
import java.time.LocalDateTime

case class Consequences(rc: DeprecatedRuntimeETLContext, batchId: String) extends SingleETL(rc) {

  implicit val logger: Logger = log

  override val mainDestination: DatasetConf = conf.getDataset("normalized_consequences")
  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val raw_variant_calling_somatic_tumor_only: DatasetConf = conf.getDataset("raw_snv_somatic_tumor_only")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      raw_variant_calling.id -> vcf(raw_variant_calling.location.replace("{{BATCH_ID}}", batchId), None, optional = true),
      raw_variant_calling_somatic_tumor_only.id -> vcf(raw_variant_calling_somatic_tumor_only.location.replace("{{BATCH_ID}}", batchId), None, optional = true)
    )
  }

  private def getVCF(data: Map[String, DataFrame]) = {

    val vcfGermline = data(raw_variant_calling.id)
    val vcfSomaticTumorOnly = data(raw_variant_calling_somatic_tumor_only.id)

    if (!vcfGermline.isEmpty) {
      vcfGermline.where(col("contigName").isin(validContigNames: _*))
    } else if (!vcfSomaticTumorOnly.isEmpty) {
      vcfSomaticTumorOnly.where(col("contigName").isin(validContigNames: _*))
    } else {
      throw new Exception("Not valid raw VCF available")
    }
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    val inputVCF = getVCF(data)

    val df = inputVCF
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
      .groupByLocus(ensembl_transcript_id) // To avoid duplicate consequences in case of multiple runs in a batch
      .agg(
        first("end") as "end",
        first("name") as "name",
        first("annotation") as "annotation"
      )
      .select($"*",
        consequences,
        impact,
        symbol,
        ensembl_gene_id,
        ensembl_feature_id,
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


  override def publish(): Unit = {
    compact(mainDestination, RepartitionByColumns(Seq("chromosome"), Some(10), Seq("start")))
    vacuum(mainDestination, 2)
  }

}

object Consequences {
  @main
  def run(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = {
    Consequences(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
