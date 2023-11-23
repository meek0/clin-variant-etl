package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.utils.Region
import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

case class CNV(rc: DeprecatedRuntimeETLContext) extends SingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("enriched_cnv")
  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val normalized_cnv_somatic_tumor_only: DatasetConf = conf.getDataset("normalized_cnv_somatic_tumor_only")
  val refseq_annotation: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val cnvRegion: Region = Region(col("cnv.chromosome"), col("cnv.start"), col("cnv.end"))
  val genes: DatasetConf = conf.getDataset("enriched_genes")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      normalized_cnv.id -> normalized_cnv.read,
      normalized_cnv_somatic_tumor_only.id -> normalized_cnv_somatic_tumor_only.read,
      refseq_annotation.id -> refseq_annotation.read,
      normalized_panels.id -> normalized_panels.read,
      genes.id -> genes.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val cnv = data(normalized_cnv.id).unionByName(data(normalized_cnv_somatic_tumor_only.id), allowMissingColumns = true)
    val refseq = data(refseq_annotation.id)

    val joinedWithPanels = joinWithPanels(cnv, refseq, data(normalized_panels.id))
    val joinedWithExons = joinWithExons(joinedWithPanels, refseq)
    val joinedWithGenes = joinWithGenes(joinedWithExons, data(genes.id));

    val groupedCnv = joinedWithGenes
      .select(struct($"cnv.*") as "cnv", struct($"refseq_genes.gene" as "symbol", $"refseq_genes.refseq_id" as "refseq_id", $"gene_length", $"overlap_bases", $"overlap_cnv_ratio", $"overlap_gene_ratio", $"panels", $"hpo", $"omim", $"orphanet", $"ddd", $"cosmic") as "gene")
      .groupBy($"cnv.chromosome", $"cnv.start", $"cnv.reference", $"cnv.alternate", $"cnv.aliquot_id", $"gene.symbol")
      .agg(first($"cnv") as "cnv", when($"gene.symbol".isNotNull, first($"gene")).otherwise(null) as "gene", count(lit(1) )as "overlap_exons")
      .select($"cnv", when($"gene".isNotNull, struct($"gene.*", $"overlap_exons")).otherwise(null) as "gene")
      .groupBy($"cnv.chromosome", $"cnv.start", $"cnv.reference", $"cnv.alternate", $"cnv.aliquot_id")
      .agg(first($"cnv") as "cnv", collect_list($"gene") as "genes")
      .select($"cnv.*", $"genes")
      .withColumn("number_genes", size($"genes"))
      .withColumn("hash", sha1(concat_ws("-", col("name"), col("aliquot_id"))))

    groupedCnv
  }

  def joinWithGenes(cnv: DataFrame, genes: DataFrame): DataFrame = {
    cnv
      .join(genes, cnv("cnv.chromosome") === genes("chromosome") && cnv("refseq_genes.gene") === genes("symbol"), "left")
  }

  def joinWithPanels(cnv: DataFrame, refseq: DataFrame, panels: DataFrame): DataFrame = {
    val refseqGenes = refseq.where($"type" === "gene")
      .select($"seqId" as "refseq_id", $"chromosome", $"start", $"end", $"gene")
    val joinedPanels = panels.select("symbol", "panels")
    val refseqGenesWithPanels = refseqGenes
      .join(joinedPanels, $"gene" === $"symbol", "left")
      .drop(joinedPanels("symbol"))
    val geneRegion = Region($"refseq_genes.chromosome", $"refseq_genes.start", $"refseq_genes.end")
    val cnvOverlap = when($"refseq_genes.gene".isNull, null)
      .otherwise(cnvRegion.overlap(geneRegion))
    cnv.as("cnv")
      .join(refseqGenesWithPanels.alias("refseq_genes"), cnvRegion.isOverlapping(geneRegion), "left")
      .withColumn("overlap_bases", cnvOverlap)
      .drop("refseq_genes.chromosome", "refseq_genes.start", "refseq_genes.end")
      .withColumn("overlap_gene_ratio", $"overlap_bases" / geneRegion.nbBases)
      .withColumn("overlap_cnv_ratio", $"overlap_bases" / cnvRegion.nbBases)
      .withColumn("gene_length", geneRegion.nbBases)
  }

  def joinWithExons(cnv: DataFrame, refseq: DataFrame): DataFrame = {
    val refseqExons = refseq.where($"type" === "exon" and $"tag" === "MANE Select")
      .select($"seqId" as "refseq_id", $"chromosome", $"start", $"end", $"gene")

    cnv
      .join(refseqExons.alias("refseq_exons"),
        $"refseq_genes.gene" === $"refseq_exons.gene" and
          cnvRegion.isOverlapping(Region($"refseq_exons.chromosome", $"refseq_exons.start", $"refseq_exons.end"))
        , "left")

      .drop("refseq_exons.start", "refseq_exons.end", "refseq_exons.chromosome")
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n=Some(1), sortColumns = Seq("start"))

}

object CNV {
  @main
  def run(rc: DeprecatedRuntimeETLContext): Unit = {
    CNV(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

