package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.utils.ClassGenerator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

class ConsequencesSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  implicit val localConf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  val job1 = new Consequences("BAT1")
  val job2 = new Consequences("BAT2")

  import spark.implicits._
  val raw_variant_calling: DatasetConf = localConf.getDataset("raw_snv")

  val data = Map(
    raw_variant_calling.id -> Seq(VCFInput()).toDF()
  )

  override def beforeAll(): Unit = {
    HadoopFileSystem.remove(job1.destination.location)
  }

  "consequences job" should "transform data in expected format" in {

    val resultDf = job1.transform(data)
    val result = resultDf.as[NormalizedConsequences].collect().head

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedConsequences", resultDf, "src/test/scala/")
    result shouldBe
      NormalizedConsequences(
        `created_on` = result.`created_on`,
        `updated_on` = result.`updated_on`,
        `normalized_consequences_oid` = result.`normalized_consequences_oid`)
  }

  "consequences job" should "run" in {

    val firstLoad = Seq(VCFInput(
      `names` = List("rs200676709")
    )).toDF()
    val secondLoad = Seq(VCFInput(
      `names` = List("rs200676710")
    )).toDF()
    val date1 = LocalDateTime.of(2021, 1, 1, 1, 1, 1)
    val date2 = LocalDateTime.of(2021, 1, 2, 1, 1, 1)


    val job1Df = job1.transform(Map(raw_variant_calling.id -> firstLoad), currentRunDateTime = date1)
    job1Df.as[NormalizedConsequences].collect() should contain allElementsOf Seq(
      NormalizedConsequences(
        `created_on` = Timestamp.valueOf(date1),
        `updated_on` = Timestamp.valueOf(date1),
        `normalized_consequences_oid` = Timestamp.valueOf(date1))
    )

    job1.load(job1Df)

    val job2Df = job2.transform(Map(raw_variant_calling.id -> secondLoad), currentRunDateTime = date2)
    job2.load(job2Df)
    val resultDf = job2.destination.read

    resultDf.as[NormalizedConsequences].collect() should contain allElementsOf Seq(
      NormalizedConsequences(
        `batch_id` = "BAT2",
        `name` = "rs200676710",
        `created_on` = Timestamp.valueOf(date1),
        `updated_on` = Timestamp.valueOf(date2),
        `normalized_consequences_oid` = Timestamp.valueOf(date2))
    )
  }
}
