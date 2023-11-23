package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.CoverageByGene.transformSingleCoverage
import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.GenomicOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumnNames}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

case class CoverageByGene(rc: DeprecatedRuntimeETLContext) extends SingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_coverage_by_gene")
  val refseq_annotation: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val normalized_coverage_by_gene: DatasetConf = conf.getDataset("normalized_coverage_by_gene")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val genes: DatasetConf = conf.getDataset("enriched_genes")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      refseq_annotation.id -> refseq_annotation.read,
      normalized_coverage_by_gene.id -> normalized_coverage_by_gene.read,
      normalized_panels.id -> normalized_panels.read,
      genes.id -> genes.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    transformSingleCoverage(data(refseq_annotation.id),
      data(normalized_coverage_by_gene.id),
      data(genes.id),
      data(normalized_panels.id),
    )
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(1), sortColumns = Seq("start"))
}

object CoverageByGene {

  def transformSingleCoverage(refseq: DataFrame, coverage: DataFrame, genes: DataFrame, panels: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val refseqGenes = refseq.filter(col("type") === "gene")

    val refseqGenesWithoutChrY = refseqGenes
      .orderBy(col("gene"), col("chromosome")) // chr X entries first
      .dropDuplicates("gene")

    val joinWithAnnotation = coverage
      .join(refseqGenesWithoutChrY, Seq("gene"), "left")
      .select(coverage("*"), refseq("start"), refseq("end"), refseq("chromosome"))
      .withColumn("start", col("start").cast(LongType))
      .withColumn("end", col("end").cast(LongType))

    val joinedWithGenes = joinWithAnnotation
      .join(genes, joinWithAnnotation("gene") === genes("symbol"), "left")
      .select(joinWithAnnotation("*"), genes("omim_gene_id"), genes("ensembl_gene_id"))

    val joinedWithPanels = joinedWithGenes
      .join(panels, joinedWithGenes("gene") === panels("symbol"), "left")
      .select(joinedWithGenes("*"), panels("panels"))

    val withHash = joinedWithPanels
      .withColumn("hash", sha1(concat_ws("-", col("batch_id"), col("aliquot_id"), col("gene"), col("chromosome"))))

    withHash
  }

  @main
  def run(rc: DeprecatedRuntimeETLContext): Unit = {
    CoverageByGene(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
