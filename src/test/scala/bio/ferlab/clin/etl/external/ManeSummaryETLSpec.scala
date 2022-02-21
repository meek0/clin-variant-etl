package bio.ferlab.clin.etl.external

import bio.ferlab.clin.model.{ManeSummaryInput, ManeSummaryOutput}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import bio.ferlab.datalake.spark3.utils.ClassGenerator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ManeSummaryETLSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(
      StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("clin_import", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)
    ))

  import spark.implicits._

  val raw_mane_summary: DatasetConf = conf.getDataset("raw_mane_summary")
  val normalized_mane_summary: DatasetConf = conf.getDataset("normalized_mane_summary")

  override def beforeAll(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${raw_mane_summary.table.map(_.database).getOrElse("clin")}")
    HadoopFileSystem.remove(new ManeSummaryETL().mainDestination.location)
  }

  val data = Map(
    raw_mane_summary.id -> Seq(ManeSummaryInput()).toDF
  )

  "mane_summary job" should "transform data in expected format" in {
    val dfs = new ManeSummaryETL().transform(data)
    dfs(normalized_mane_summary.id).as[ManeSummaryOutput].collect().head shouldBe ManeSummaryOutput()
    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "ManeSummaryOutput", dfs(normalized_mane_summary.id), "src/test/scala/")
  }
}
