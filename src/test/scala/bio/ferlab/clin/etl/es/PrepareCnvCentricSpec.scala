package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrepareCnvCentricSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  val data = Map(
    enriched_cnv.id -> Seq(CnvEnrichedOutput("1"), CnvEnrichedOutput("2")).toDF,
  )

  "Cnv_centric transform" should "return data as CnvCentricOutput" in {

    val result = new PrepareCnvCentric("re_000").transformSingle(data);
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "CnvCentricOutput", result, "src/test/scala/")

    result.count() shouldBe 2
    result.as[CnvCentricOutput].collect() should contain allElementsOf Seq(CnvCentricOutput("1"), CnvCentricOutput("2"))
  }

}
