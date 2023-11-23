package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.model.enriched.EnrichedCoverageByGene
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.testutils.{SparkSpec, DeprecatedTestETLContext}

class PrepareCoverageByGeneCentricSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val enriched_coverage_by_gene: DatasetConf = conf.getDataset("enriched_coverage_by_gene")

  val data = Map(
    enriched_coverage_by_gene.id -> Seq(EnrichedCoverageByGene("1"), EnrichedCoverageByGene("2")).toDF,
  )

  "coverage_by_gene_centric transform" should "return data as CoverageByGeneCentricOutput" in {

    val result = PrepareCoverageByGeneCentric(DeprecatedTestETLContext(), "re_000").transformSingle(data);
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "CnvCentricOutput", result, "src/test/scala/")

    result.count() shouldBe 2
    result.as[CoverageByGeneCentricOutput].collect() should contain allElementsOf Seq(CoverageByGeneCentricOutput("1"), CoverageByGeneCentricOutput("2"))
  }

}
