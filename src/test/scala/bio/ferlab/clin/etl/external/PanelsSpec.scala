package bio.ferlab.clin.etl.external

import bio.ferlab.clin.model.PanelOutput
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PanelsSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(
      StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("clin_import", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)
    ))

  import spark.implicits._

  val raw_panels: DatasetConf = conf.getDataset("raw_panels")
  val job1 = new Panels()

  override def beforeAll(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${raw_panels.table.map(_.database).getOrElse("clin")}")
    HadoopFileSystem.remove(job1.destination.location)
  }

  val data = Map(
    raw_panels.id -> raw_panels.read
  )

  "panels job" should "transform data in expected format" in {
    val resultDf =  job1.transform(data).as[PanelOutput]
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "PanelOutput", df, "src/test/scala/")
    resultDf.show(false)
    
    val result = resultDf.collect().head

    result shouldBe PanelOutput()
  }

}
