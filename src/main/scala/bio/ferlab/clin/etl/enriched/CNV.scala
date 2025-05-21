package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.enriched.CNV._
import bio.ferlab.clin.etl.mainutils.OptionalBatch
import bio.ferlab.clin.etl.utils.ClinicalUtils.getAnalysisServiceRequestIdsInBatch
import bio.ferlab.clin.etl.utils.{FrequencyUtils, Region}
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.transformation.DropDuplicates
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions.{struct, _}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime

case class CNV(rc: RuntimeETLContext, batchId: Option[String]) extends SimpleSingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("enriched_cnv")
  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val normalized_cnv_somatic_tumor_only: DatasetConf = conf.getDataset("normalized_cnv_somatic_tumor_only")
  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv")
  val refseq_annotation: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")
  val nextflow_svclustering: DatasetConf = conf.getDataset("nextflow_svclustering")
  val nextflow_svclustering_parental_origin: DatasetConf = conf.getDataset("nextflow_svclustering_parental_origin")
  val normalized_gnomad_cnv_v4: DatasetConf = conf.getDataset("normalized_gnomad_cnv_v4")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    val extractedData = Map(
      refseq_annotation.id -> refseq_annotation.read,
      normalized_panels.id -> normalized_panels.read,
      genes.id -> genes.read,
      nextflow_svclustering.id -> nextflow_svclustering.read,
      normalized_gnomad_cnv_v4.id -> normalized_gnomad_cnv_v4.read
    )
    val clinicalDf = enriched_clinical.read

    batchId match {
      case Some(id) =>
        // If a batch id was submitted, only process the specified id
        val normalizedCnvDf = normalized_cnv.read.where($"batch_id" === id)
        val normalizedCnvSomaticTumorOnlyDf = normalized_cnv_somatic_tumor_only.read.where($"batch_id" === id)
        val normalizedSnvDf = normalized_snv.read.where($"batch_id" === id)

        val analysisServiceRequestIds: Seq[String] = getAnalysisServiceRequestIdsInBatch(clinicalDf, id)
        val nextflowSVClusteringParentalOrigin = nextflow_svclustering_parental_origin
          .read.where($"analysis_service_request_id".isin(analysisServiceRequestIds: _*))

        Map(
          normalized_cnv.id -> normalizedCnvDf,
          normalized_cnv_somatic_tumor_only.id -> normalizedCnvSomaticTumorOnlyDf,
          normalized_snv.id -> normalizedSnvDf,
          nextflow_svclustering_parental_origin.id -> nextflowSVClusteringParentalOrigin
        ) ++ extractedData

      case None =>
        // If no batch id was submitted, process all data
        Map(
          normalized_cnv.id -> normalized_cnv.read,
          normalized_cnv_somatic_tumor_only.id -> normalized_cnv_somatic_tumor_only.read,
          normalized_snv.id -> normalized_snv.read,
          nextflow_svclustering_parental_origin.id -> nextflow_svclustering_parental_origin.read
        ) ++ extractedData
    }
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minValue,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {

    val cnvDf = data(normalized_cnv.id).unionByName(data(normalized_cnv_somatic_tumor_only.id), allowMissingColumns = true)
    val snvDf = data(normalized_snv.id)
    val refseqDf = data(refseq_annotation.id)
    val panelsDf = data(normalized_panels.id)
    val genesDf = data(genes.id)
    val svclusteringDf = data(nextflow_svclustering.id)
    val parentalOriginDf = data(nextflow_svclustering_parental_origin.id)
    val gnomadV4 = data(normalized_gnomad_cnv_v4.id)

    cnvDf
      .withPanels(refseqDf, panelsDf)
      .withExons(refseqDf)
      .withGenes(genesDf)
      .withParentalOrigin(parentalOriginDf)
      .withFrequencies(svclusteringDf, gnomadV4)
      .withClinVariantExternalReference
      .withSnvCount(snvDf)
      .withColumn("number_genes", size($"genes"))
      .withColumn("hash", sha1(concat_ws("-", col("name"), col("alternate"), col("service_request_id")))) // if changed then modify + run https://github.com/Ferlab-Ste-Justine/clin-pipelines/blob/master/src/main/scala/bio/ferlab/clin/etl/scripts/FixFlagHashes.scala

  }
}

object CNV {
  private final val CnvRegion: Region = Region(col("cnv.chromosome"), col("cnv.start"), col("cnv.end"))
  private final val SvClusteringRegion: Region = Region(col("cnv.cluster_info.chromosome"), col("cnv.cluster_info.start"), col("cnv.cluster_info.end"))
  private final val SvClusteringDropDuplicates = new DropDuplicates(
    Seq("cnv.chromosome", "cnv.start", "cnv.reference", "cnv.alternate"),
    col("overlap_fraction_gnomad").desc,
    col("gnomad.sf").desc
  )

  implicit class DataFrameOps(df: DataFrame) {

    def withSnvCount(snv: DataFrame): DataFrame = {
      withCount(df, "name", snv, "hgvsg", "snv_count")
    }

    def withPanels(refseq: DataFrame, panels: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val refseqGenes = refseq.where($"type" === "gene")
        .select($"seqId" as "refseq_id", $"chromosome", $"start", $"end", $"gene" as "symbol")
      val joinedPanels = panels.select("symbol", "panels")
      val refseqGenesWithPanels = refseqGenes
        .join(joinedPanels, Seq("symbol"), "left")
      val geneRegion = Region($"refseq_genes.chromosome", $"refseq_genes.start", $"refseq_genes.end")
      val cnvOverlap = when($"symbol".isNull, null)
        .otherwise(CnvRegion.overlap(geneRegion))

      df.alias("cnv")
        .join(refseqGenesWithPanels.alias("refseq_genes"), CnvRegion.isOverlapping(geneRegion), "left")
        .withColumn("overlap_bases", cnvOverlap)
        .withColumn("overlap_gene_ratio", $"overlap_bases" / geneRegion.nbBases)
        .withColumn("overlap_cnv_ratio", $"overlap_bases" / CnvRegion.nbBases)
        .withColumn("gene_length", geneRegion.nbBases)
        .select(
          df("*"),
          $"symbol",
          $"refseq_id",
          $"panels",
          $"overlap_bases",
          $"overlap_gene_ratio",
          $"overlap_cnv_ratio",
          $"gene_length"
        )
    }

    def withExons(refseq: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val refseqExons = refseq
        .where($"type" === "exon" and $"tag" === "MANE Select")
        .select("chromosome", "start", "end", "gene")

      // Add exons to later group by occurrence+symbol and count overlap exons
      df.alias("cnv")
        .join(refseqExons,
          df("symbol") === refseqExons("gene")
            and CnvRegion.isOverlapping(Region(refseqExons("chromosome"), refseqExons("start"), refseqExons("end"))),
          "left")
        .select(df("*"))
    }

    def withGenes(genes: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      df
        .join(genes, Seq("chromosome", "symbol"), "left")
        .select(
          df("*"),
          struct("symbol",
            "refseq_id",
            "omim_gene_id",
            "gene_length",
            "overlap_bases",
            "overlap_cnv_ratio",
            "overlap_gene_ratio",
            "panels",
            "hpo",
            "location",
            "omim",
            "orphanet",
            "ddd",
            "cosmic") as "gene"
        )
        .groupByLocus($"aliquot_id", $"gene.symbol")
        .agg(
          first(struct(df("*"))) as "cnv",
          when($"gene.symbol".isNotNull, first($"gene")).otherwise(null) as "gene",
          count(lit(1)) as "overlap_exons"
        )
        .withColumn("gene", when($"gene".isNotNull, struct($"gene.*", $"overlap_exons")).otherwise(null))
        .groupByLocus($"aliquot_id")
        .agg(
          first($"cnv") as "cnv",
          collect_list($"gene") as "genes"
        )
        .select($"cnv.*", $"genes")
    }

    def withParentalOrigin(parentalOrigin: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      df
        .join(parentalOrigin,
          df("service_request_id") === parentalOrigin("service_request_id") and array_contains(parentalOrigin("members"), df("name")),
          "left")
        .select(df("*"), $"transmission", $"parental_origin")
    }

    def withFrequencies(svclustering: DataFrame, gnomadV4: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val clusterDf = df
        .join(svclustering, array_contains(svclustering("members"), df("name")), "left")
        .select(
          df("*"),
          $"frequency_RQDM",
          struct(
            svclustering("name") as "id",
            FrequencyUtils.EmptyClusterFrequencies
          ) as "cluster",
          struct( // temporary struct to help frequencies joins
            svclustering("chromosome") as "chromosome",
            svclustering("start") as "start",
            svclustering("end") as "end",
            svclustering("reference") as "reference",
            svclustering("alternate") as "alternate",
          ) as "cluster_info"
        )

      joinSvClusteringOverlappingGnomad(clusterDf, gnomadV4)
        .drop("cluster_info")
    }

    def joinSvClusteringOverlappingGnomad(clusterDf: DataFrame, gnomadV4: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val gnomadRegion = Region($"gnomad.chromosome", $"gnomad.start", $"gnomad.end")

      val overlapWithMetricsDF = clusterDf.as("cnv")
        .join(gnomadV4.as("gnomad"), SvClusteringRegion.isOverlapping(gnomadRegion), "left")
        .withColumn("overlap_length",
          least(col("cnv.cluster_info.end"), col("gnomad.end")) - greatest(col("cnv.cluster_info.start"), col("gnomad.start")) + 1
        )
        .withColumn("length_gnomad", col("gnomad.end") - col("gnomad.start") + 1)
        .withColumn("overlap_fraction_gnomad", col("overlap_length") / col("length_gnomad"))

      val distinctOverlapWithMetricsDF = SvClusteringDropDuplicates.transform(overlapWithMetricsDF)

      val withThresholdDf = distinctOverlapWithMetricsDF.filter(
        col("overlap_fraction_gnomad") >= Region.GnomadV4CNVOverlapThreshold
      ).select(
        $"cnv.chromosome" as "chromosome",
        $"cnv.start" as "start",
        $"cnv.reference" as "reference",
        $"cnv.alternate" as "alternate",
        struct(
          $"gnomad.sc" as "sc",
          $"gnomad.sn" as "sn",
          $"gnomad.sf" as "sf",
        ) as "gnomad_exomes_4"
      )

      clusterDf.joinByLocus(withThresholdDf, "left")
        .select(
          clusterDf("*"),
          $"gnomad_exomes_4"
        )
        .withColumn("cluster", $"cluster".withField("external_frequencies.gnomad_exomes_4", $"gnomad_exomes_4"))
        .drop("gnomad_exomes_4")
    }

    def withClinVariantExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val conditionValueMap: List[(Column, String)] = List(
        ($"cluster.external_frequencies.gnomad_exomes_4".isNotNull) -> "gnomAD"
      )

      withExternalReference(df, conditionValueMap)
    }
  }

  @main
  def run(rc: RuntimeETLContext, batch: OptionalBatch): Unit = {
    CNV(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

