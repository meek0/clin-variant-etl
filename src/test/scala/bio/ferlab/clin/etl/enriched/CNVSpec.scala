package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.utils.ClassGenerator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.explode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CNVSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers {

  import spark.implicits._

  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val normalized_refseq_annotation: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val normalized_panels: DatasetConf = conf.getDataset("normalized_panels")

  val refSeq = Seq(
    NormalizedRefSeq(),
  ).toDF()

  val panels = Seq(
    NormalizedPanels(),
  ).toDF()

  val data: Map[String, DataFrame] = Map(
    normalized_cnv.id -> Seq(NormalizedCNV()).toDF(),
    normalized_refseq_annotation.id -> refSeq,
    normalized_panels.id -> panels,
  )

  "Enriched CNV job" should "enriched data" in {

    val results = new CNV().transformSingle(data).as[EnrichedCNV]

    //val res = results.select(explode($"genes")).select("col.*")
    //val genes = results.select(explode($"genes")).select("col.*")
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "EnrichedCNV", res, "src/test/scala/")
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "EnrichedCNVgenes", genes, "src/test/scala/")

    results.collect() should contain theSameElementsAs Seq(
      EnrichedCNV(),
    )
  }

}

