package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.model.normalized.NormalizedPanels
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, DeprecatedTestETLContext}

class PanelsSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val raw_panels: DatasetConf = conf.getDataset("raw_panels")
  val job1 = Panels(DeprecatedTestETLContext())

  override val dbToCreate: List[String] = List(raw_panels.table.map(_.database).getOrElse("clin"))
  override val dsToClean: List[DatasetConf] = List(job1.mainDestination)

  private val data = Map(
    raw_panels.id -> raw_panels.read
  )

  "panels job" should "transform data in expected format" in {
    val resultDf =  job1.transformSingle(data).as[NormalizedPanels]
    
    val result = resultDf.collect().head

    result shouldBe NormalizedPanels()
  }

}
