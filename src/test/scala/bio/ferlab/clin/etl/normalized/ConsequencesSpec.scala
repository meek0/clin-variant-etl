package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
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

class ConsequencesSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  val job1 = new Consequences("BAT1")
  val job2 = new Consequences("BAT2")

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")

  val data = Map(
    raw_variant_calling.id -> Seq(VCF_SNV_Input()).toDF()
  )

  val data_with_duplicates = Map(
    raw_variant_calling.id -> Seq(
      VCF_SNV_Input(`INFO_CSQ` = List(bio.ferlab.clin.model.INFO_CSQ(`Feature` = "bar"))),  // not duplicated
      VCF_SNV_Input(`INFO_CSQ` = List(bio.ferlab.clin.model.INFO_CSQ(`Feature` = "foo"))),  // duplicated with bellow
      VCF_SNV_Input(`INFO_CSQ` = List(bio.ferlab.clin.model.INFO_CSQ(`Feature` = "foo"))),
      VCF_SNV_Input(`INFO_CSQ` = List(bio.ferlab.clin.model.INFO_CSQ(`Feature` = null))), // duplicated with bellow
      VCF_SNV_Input(`INFO_CSQ` = List(bio.ferlab.clin.model.INFO_CSQ(`Feature` = null))),
    ).toDF()
  )

  override def beforeAll(): Unit = {
    HadoopFileSystem.remove(job1.mainDestination.location)
  }

  "consequences job" should "transform data in expected format" in {
    val results = job1.transform(data)
    val resultDf = results(job1.mainDestination.id)
    val result = resultDf.as[NormalizedConsequences].collect().head

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedConsequences", resultDf, "src/test/scala/")
    result shouldBe
      NormalizedConsequences(
        `created_on` = result.`created_on`,
        `updated_on` = result.`updated_on`,
        `normalized_consequences_oid` = result.`normalized_consequences_oid`)
  }


  "consequences job" should "remove duplicates" in {
    val results = job1.transform(data_with_duplicates)
    val resultDf = results(job1.mainDestination.id)
    val result = resultDf.as[NormalizedConsequences].collect()

    result should contain theSameElementsAs Seq(
      NormalizedConsequences(
        `ensembl_feature_id`= "bar",
        `ensembl_transcript_id`= "bar",
        `created_on` = result.head.`created_on`,
        `updated_on` = result.head.`updated_on`,
        `normalized_consequences_oid` = result.head.`normalized_consequences_oid`),
      NormalizedConsequences(
        `ensembl_feature_id`= "foo",
        `ensembl_transcript_id`= "foo",
        `created_on` = result.head.`created_on`,
        `updated_on` = result.head.`updated_on`,
        `normalized_consequences_oid` = result.head.`normalized_consequences_oid`),
      NormalizedConsequences(
        `ensembl_feature_id`= null,
        `ensembl_transcript_id`= null,
        `created_on` = result.head.`created_on`,
        `updated_on` = result.head.`updated_on`,
        `normalized_consequences_oid` = result.head.`normalized_consequences_oid`)
    )
  }

  "consequences job" should "run" in {

    val firstLoad = Seq(VCF_SNV_Input(
      `names` = List("rs200676709")
    )).toDF()
    val secondLoad = Seq(VCF_SNV_Input(
      `names` = List("rs200676710")
    )).toDF()
    val date1 = LocalDateTime.of(2021, 1, 1, 1, 1, 1)
    val date2 = LocalDateTime.of(2021, 1, 2, 1, 1, 1)


    val job1Results = job1.transform(Map(raw_variant_calling.id -> firstLoad), currentRunDateTime = date1)
    val job1Df = job1Results(job1.mainDestination.id)
    job1Df.as[NormalizedConsequences].collect() should contain allElementsOf Seq(
      NormalizedConsequences(
        `created_on` = Timestamp.valueOf(date1),
        `updated_on` = Timestamp.valueOf(date1),
        `normalized_consequences_oid` = Timestamp.valueOf(date1))
    )

    job1.load(job1Results)

    val job2Results = job2.transform(Map(raw_variant_calling.id -> secondLoad), currentRunDateTime = date2)
    job2.load(job2Results)
    val resultDf = job2.mainDestination.read

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
