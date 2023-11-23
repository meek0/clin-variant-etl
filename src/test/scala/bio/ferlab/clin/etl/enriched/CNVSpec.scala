package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model._
import bio.ferlab.clin.model.normalized.{NormalizedCNV, NormalizedCNVSomaticTumorOnly, NormalizedPanels, NormalizedRefSeq}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.models.enriched.EnrichedGenes
import bio.ferlab.datalake.testutils.{SparkSpec, DeprecatedTestETLContext}
import org.apache.spark.sql.DataFrame

class CNVSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val normalized_cnv_somatic_tumor_only: DatasetConf = conf.getDataset("normalized_cnv_somatic_tumor_only")
  val normalized_refseq_annotation: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")
  val genes: DatasetConf = conf.getDataset("enriched_genes")

  val job = CNV(DeprecatedTestETLContext())

  val refSeq = Seq(
    NormalizedRefSeq(),
  ).toDF()

  val panels = Seq(
    NormalizedPanels(),
  ).toDF()

  val genesDf: DataFrame = Seq(EnrichedGenes()).toDF()

  val data: Map[String, DataFrame] = Map(
    normalized_cnv.id -> Seq(NormalizedCNV()).toDF(),
    normalized_cnv_somatic_tumor_only.id -> Seq(NormalizedCNVSomaticTumorOnly(`aliquot_id` = "11112")).toDF(),
    normalized_refseq_annotation.id -> refSeq,
    normalized_panels.id -> panels,
    genes.id -> genesDf,
  )

  val refSeq_no_genes = Seq(
    NormalizedRefSeq(`chromosome` = "42"),
  ).toDF()

  val data_no_genes_chr_2: Map[String, DataFrame] = Map(
    normalized_cnv.id -> Seq(NormalizedCNV()).toDF(),
    normalized_cnv_somatic_tumor_only.id -> spark.emptyDataFrame,
    normalized_refseq_annotation.id -> refSeq_no_genes,
    normalized_panels.id -> panels,
    genes.id -> genesDf,
  )

  "Enriched CNV job" should "enriched data" in {

    val results = job.transformSingle(data).as[CnvEnrichedOutput]

    //val res = results.select(explode($"genes")).select("col.*")
    //val genes = results.select(explode($"genes")).select("col.*")
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "EnrichedCNV", res, "src/test/scala/")
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "EnrichedCNVgenes", genes, "src/test/scala/")
    // results.write.mode(SaveMode.Overwrite).json("results.json")

    results.collect() should contain theSameElementsAs Seq(
      CnvEnrichedOutput(),
      CnvEnrichedOutput(`aliquot_id` = "11112", `variant_type` = "somatic_tumor_only", `cn` = None, `hash` = "3802349cec5a9cac34daf58dd5a63ee05d7b2f1e"),
    )
  }

  "Enriched CNV job" should "have number_genes = 0 if no genes" in {

    val results = job.transformSingle(data_no_genes_chr_2).as[CnvEnrichedOutput]

    results.collect() should contain theSameElementsAs Seq(
      CnvEnrichedOutput(`genes` = List(), `number_genes` = 0),
    )
  }

}

