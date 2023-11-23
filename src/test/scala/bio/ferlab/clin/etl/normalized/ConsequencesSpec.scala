package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.model.normalized.NormalizedConsequences
import bio.ferlab.clin.etl.model.raw
import bio.ferlab.clin.etl.model.raw.{INFO_CSQ, VCF_SNV_Input, VCF_SNV_Somatic_Input}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, SparkSpec, DeprecatedTestETLContext}

import java.sql.Timestamp
import java.time.LocalDateTime

class ConsequencesSpec extends SparkSpec with WithTestConfig with CleanUpBeforeAll {

  val job1 = Consequences(DeprecatedTestETLContext(), "BAT1")
  val job2 = Consequences(DeprecatedTestETLContext(), "BAT2")

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val raw_variant_calling_somatic_tumor_only: DatasetConf = conf.getDataset("raw_snv_somatic_tumor_only")

  override val dsToClean: List[DatasetConf] = List(job1.mainDestination)

  val data = Map(
    raw_variant_calling.id -> Seq(VCF_SNV_Input()).toDF(),
    raw_variant_calling_somatic_tumor_only.id -> spark.emptyDataFrame,
  )

  val data_with_duplicates = data ++ Map(
    raw_variant_calling.id -> Seq(
      VCF_SNV_Input(`INFO_CSQ` = List(INFO_CSQ(`Feature` = "bar"))),  // not duplicated
      VCF_SNV_Input(`INFO_CSQ` = List(raw.INFO_CSQ(`Feature` = "foo"))),  // duplicated with bellow
      VCF_SNV_Input(`INFO_CSQ` = List(raw.INFO_CSQ(`Feature` = "foo"))),
      VCF_SNV_Input(`INFO_CSQ` = List(raw.INFO_CSQ(`Feature` = null))), // duplicated with bellow
      VCF_SNV_Input(`INFO_CSQ` = List(raw.INFO_CSQ(`Feature` = null))),
    ).toDF(),
  )

  val dataSomaticTumorOnly= data ++ Map(
    raw_variant_calling.id -> spark.emptyDataFrame,
    raw_variant_calling_somatic_tumor_only.id -> Seq(VCF_SNV_Somatic_Input(), VCF_SNV_Somatic_Input(), VCF_SNV_Somatic_Input()).toDF(),
  )

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


    val job1Results = job1.transform(data ++ Map(raw_variant_calling.id -> firstLoad), currentRunDateTime = date1)
    val job1Df = job1Results(job1.mainDestination.id)
    job1Df.as[NormalizedConsequences].collect() should contain allElementsOf Seq(
      NormalizedConsequences(
        `created_on` = Timestamp.valueOf(date1),
        `updated_on` = Timestamp.valueOf(date1),
        `normalized_consequences_oid` = Timestamp.valueOf(date1))
    )

    job1.load(job1Results)

    val job2Results = job2.transform(data ++ Map(raw_variant_calling.id -> secondLoad), currentRunDateTime = date2)
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

  "consequences job" should "throw exception if no valid VCF" in {
    val exception = intercept[Exception] {
      job1.transform(data ++ Map(raw_variant_calling.id -> spark.emptyDataFrame,
        raw_variant_calling_somatic_tumor_only.id -> spark.emptyDataFrame))
    }
    exception.getMessage shouldBe "Not valid raw VCF available"

  }

  "consequences job" should "transform data somatic tumor only in expected format" in {
    val results = job1.transform(dataSomaticTumorOnly)
    val resultDf = results("normalized_consequences")
    val result = resultDf.as[NormalizedConsequences].collect()
    result.length shouldBe 1
  }

  "consequences job" should "ignore invalid contigName in VCF Germline" in {
    val results = job1.transform(data ++ Map(raw_variant_calling.id -> Seq(
      VCF_SNV_Input(`contigName` = "chr2"),
      VCF_SNV_Input(`contigName` = "chrY"),
      VCF_SNV_Input(`contigName` = "foo")).toDF))
    val result = results("normalized_consequences").as[NormalizedConsequences].collect()
    result.length shouldBe > (0)
    result.foreach(r => r.chromosome shouldNot be("foo"))
  }

  "consequences job" should "ignore invalid contigName in VCF Somatic tumor only" in {
    val results = job1.transform(data ++ Map(
      raw_variant_calling.id -> spark.emptyDataFrame,
      raw_variant_calling_somatic_tumor_only.id -> Seq(
        VCF_SNV_Somatic_Input(`contigName` = "chr2"),
        VCF_SNV_Somatic_Input(`contigName` = "chrY"),
        VCF_SNV_Somatic_Input(`contigName` = "foo")).toDF))
    val result = results("normalized_consequences").as[NormalizedConsequences].collect()
    result.length shouldBe > (0)
    result.foreach(r => r.chromosome shouldNot be("foo"))
  }

}
