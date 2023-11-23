package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.testutils.{SparkSpec, DeprecatedTestETLContext}

class PrepareCnvCentricSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  val data = Map(
    enriched_cnv.id -> Seq(CnvEnrichedOutput("1"), CnvEnrichedOutput("2")).toDF,
  )

  "Cnv_centric transform" should "return data as CnvCentricOutput" in {

    val result = PrepareCnvCentric(DeprecatedTestETLContext(), "re_000").transformSingle(data);
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "CnvCentricOutput", result, "src/test/scala/")

    result.count() shouldBe 2
    result.as[CnvCentricOutput].collect() should contain allElementsOf Seq(CnvCentricOutput("1"), CnvCentricOutput("2"))
  }

}
