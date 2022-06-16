package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.utils.Region
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime

class CNV()(implicit configuration: Configuration) extends ETL {

  override val destination: DatasetConf = conf.getDataset("enriched_cnv")
  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val refseq_annotation: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val cnvRegion: Region = Region(col("cnv.chromosome"), col("cnv.start"), col("cnv.end"))

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      normalized_cnv.id -> normalized_cnv.read
        .where(col("updated_on") >= Timestamp.valueOf(lastRunDateTime)),
      refseq_annotation.id -> refseq_annotation.read,
      normalized_panels.id -> normalized_panels.read
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val cnv = data(normalized_cnv.id)
    val refseq = data(refseq_annotation.id)

    val joinedWithGenes = joinWithGenes(cnv, refseq, data(normalized_panels.id))
    val joinedWithExons = joinWithExons(joinedWithGenes, refseq)

    val groupedCnv = joinedWithExons
      .select(struct($"cnv.*") as "cnv", struct($"refseq_genes.gene" as "symbol", $"refseq_genes.refseq_id" as "refseq_id", $"overlap_bases", $"overlap_cnv_ratio", $"overlap_gene_ratio", $"panels") as "gene")
      .groupBy($"cnv.chromosome", $"cnv.start", $"cnv.reference", $"cnv.alternate", $"cnv.aliquot_id", $"gene.refseq_id")
      .agg(first($"cnv") as "cnv", first($"gene") as "gene", count(lit(1)) as "overlap_exons")
      .select($"cnv", struct($"gene.*", $"overlap_exons") as "gene")
      .groupBy($"cnv.chromosome", $"cnv.start", $"cnv.reference", $"cnv.alternate", $"cnv.aliquot_id")
      .agg(first($"cnv") as "cnv", collect_list($"gene") as "genes")
      .select($"cnv.*", $"genes")
      .withColumn("number_genes", size($"genes"))
    groupedCnv
  }

  def joinWithGenes(cnv: DataFrame, refseq: DataFrame, panels: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val refseqGenes = refseq.where($"type" === "gene")
      .select($"seqId" as "refseq_id", $"chromosome", $"start", $"end", $"gene")
    val joinedPanels = panels.select("symbol", "panels")
    val refseqGenesWithPanels = refseqGenes
      .join(joinedPanels, $"gene" === $"symbol", "left")
      .drop(joinedPanels("symbol"))
    val geneRegion = Region($"refseq_genes.chromosome", $"refseq_genes.start", $"refseq_genes.end")
    val cnvOverlap = when($"refseq_genes.refseq_id".isNull, null)
      .otherwise(cnvRegion.overlap(geneRegion))
    cnv.as("cnv")
      .join(refseqGenesWithPanels.alias("refseq_genes"), cnvRegion.isOverlapping(geneRegion), "left")
      .withColumn("overlap_bases", cnvOverlap)
      .drop("refseq_genes.chromosome", "refseq_genes.start", "refseq_genes.end")
      .withColumn("overlap_gene_ratio", $"overlap_bases" / geneRegion.nbBases)
      .withColumn("overlap_cnv_ratio", $"overlap_bases" / cnvRegion.nbBases)
  }

  def joinWithExons(cnv: DataFrame, refseq: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val refseqExons = refseq.where($"type" === "exon")
      .select($"seqId" as "refseq_id", $"chromosome", $"start", $"end", $"gene")

    cnv
      .join(refseqExons.alias("refseq_exons"),
        $"refseq_genes.refseq_id" === $"refseq_exons.refseq_id" and
          cnvRegion.isOverlapping(Region($"refseq_exons.chromosome", $"refseq_exons.start", $"refseq_exons.end"))
        , "left")

      .drop("refseq_exons.start", "refseq_exons.end", "refseq_exons.chromosome")
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(1, col("chromosome"))
      .sortWithinPartitions("start"))
  }


}
