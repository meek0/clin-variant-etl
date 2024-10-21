package bio.ferlab.clin.etl.es

import bio.ferlab.clin.model._
import bio.ferlab.clin.model.enriched.EnrichedCNV
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}

class PrepareCnvCentricSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val enriched_cnv: DatasetConf = conf.getDataset("enriched_cnv")

  val data = Map(enriched_cnv.id -> Seq(EnrichedCNV("1"), EnrichedCNV("2")).toDF)

  "Cnv_centric transform" should "return data as CnvCentricOutput" in {

    val result = PrepareCnvCentric(TestETLContext()).transformSingle(data);

    result
      .as[CnvCentricOutput]
      .collect() should contain theSameElementsAs Seq(CnvCentricOutput("1"), CnvCentricOutput("2"))
  }
}
