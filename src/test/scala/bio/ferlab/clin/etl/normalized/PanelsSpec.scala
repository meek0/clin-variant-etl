package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.model.NormalizedPanels
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PanelsSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val raw_panels: DatasetConf = conf.getDataset("raw_panels")
  val job1 = new Panels()

  override def beforeAll(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${raw_panels.table.map(_.database).getOrElse("clin")}")
    HadoopFileSystem.remove(job1.mainDestination.location)
  }

  private val data = Map(
    raw_panels.id -> raw_panels.read
  )

  "panels job" should "transform data in expected format" in {
    val resultDf =  job1.transformSingle(data).as[NormalizedPanels]
    
    val result = resultDf.collect().head

    result shouldBe NormalizedPanels()
  }

}
