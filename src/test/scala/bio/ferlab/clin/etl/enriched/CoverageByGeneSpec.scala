package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model.enriched.EnrichedCoverageByGene
import bio.ferlab.clin.model.normalized.{NormalizedCoverageByGene, NormalizedPanels, NormalizedRefSeq}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.testutils.models.enriched.EnrichedGenes
import bio.ferlab.datalake.testutils.{SparkSpec, DeprecatedTestETLContext}

class CoverageByGeneSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val normalized_coverage_by_gene: DatasetConf = conf.getDataset("normalized_coverage_by_gene")
  val normalized_refseq_annotation: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val genes: DatasetConf = conf.getDataset("enriched_genes")

  val job = CoverageByGene(DeprecatedTestETLContext())

  it should "enrich data " in {
    val coverageByGeneDf = Seq(
      NormalizedCoverageByGene(
        gene = "OR4F5", size = 1,
        coverage5 = 0.3f, coverage15 = 0.52f, coverage30 = 0.52f, coverage50 = 2.5f, coverage100 = 12.3f, coverage200 = 1.23f,
        coverage300 = 1.232f, coverage400 = 3.02f, coverage500 = 1.2f, coverage1000 = 8.4f,
        aliquot_id = "aliquot1", batch_id = "test_extum")
    ).toDF()

    val refseqDf = Seq(
      NormalizedRefSeq(gene = "OR4F5", start = "10000", end = "10059", chromosome = "Y", `type` = "gene"),
      NormalizedRefSeq(gene = "OR4F5", start = "10000", end = "10059", chromosome = "X", `type` = "gene"),
      NormalizedRefSeq(gene = "OR4F5", start = "10000", end = "10059", chromosome = "12", `type` = "exon")
    ).toDF()

    val panelsDf = Seq(
      NormalizedPanels(
        symbol = "OR4F5",
        panels = List("DYSTM", "MITN"),
        version = List("MITN_v1", "NM_152490.5")
      )
    ).toDF()

    val genesDf = Seq(
      EnrichedGenes(chromosome = "12",
        ensembl_gene_id = "EBS123414",
        omim_gene_id = "1234")
    ).toDF()

    val data = Map(
      normalized_refseq_annotation.id -> refseqDf,
      normalized_coverage_by_gene.id -> coverageByGeneDf,
      genes.id -> genesDf,
      normalized_panels.id -> panelsDf,
    )
    val result = job.transformSingle(data)

    result
      .as[EnrichedCoverageByGene]
      .collect() should contain theSameElementsAs Seq(
      EnrichedCoverageByGene(gene = "OR4F5",
        size = 1,
        coverage5 = 0.3f, coverage15 = 0.52f, coverage30 = 0.52f, coverage50 = 2.5f, coverage100 = 12.3f, coverage200 = 1.23f,
        coverage300 = 1.232f, coverage400 = 3.02f, coverage500 = 1.2f, coverage1000 = 8.4f, aliquot_id = "aliquot1", batch_id = "test_extum",
        chromosome = "X", panels =  Seq("DYSTM", "MITN"), omim_gene_id = "1234", service_request_id = "SR0095",
        ensembl_gene_id = "EBS123414", start = 10000, end = 10059
    ,
    ),
    )
  }
}

