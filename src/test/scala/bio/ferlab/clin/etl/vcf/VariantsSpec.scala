package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariantsSpec extends AnyFlatSpec with WithSparkSession with Matchers with BeforeAndAfterAll {

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(
      StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("clin_import", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)
    ))

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_snv")
  val group: DatasetConf = conf.getDataset("normalized_group")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")

  val groupDf: DataFrame = Seq(
    GroupOutput(
      `id` = "FM00001",
      `members` = List(
        MEMBERS("PA0001", `affected_status` = true),
        MEMBERS("PA0002", `affected_status` = false),
        MEMBERS("PA0003", `affected_status` = false)
      )
    ),
    GroupOutput(
      `id` = "FM00001",
      `members` = List(
        MEMBERS("PA0004", `affected_status` = true)
      )
    )
  ).toDF()

  val taskDf: DataFrame = Seq(
    TaskOutput(
      `id` = "73254",
      `patient_id` = "PA0001",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`aliquot_id` = "1")
    ),
    TaskOutput(
      `id` = "73255",
      `patient_id` = "PA0002",
      `experiment` = EXPERIMENT(`aliquot_id` = "2")
    ),
    TaskOutput(
      `id` = "73256",
      `patient_id` = "PA0003",
      `experiment` = EXPERIMENT(`aliquot_id` = "3")
    ),
    TaskOutput(
      `id` = "73256",
      `patient_id` = "PA0004",
      `experiment` = EXPERIMENT(`aliquot_id` = "3")
    )
  ).toDF

  val serviceRequestDf: DataFrame = Seq(
    ServiceRequestOutput(),
    ServiceRequestOutput(`id` = "111")
  ).toDF()

  val job1 = new Variants("BAT1")
  val job2 = new Variants("BAT2")

  override def beforeAll(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${raw_variant_calling.table.map(_.database).getOrElse("clin")}")
    HadoopFileSystem.remove(job1.destination.location)
  }


  val data = Map(
    raw_variant_calling.id -> Seq(
      VCFInput(
        `genotypes` = List(
          GENOTYPES(`sampleId` = "1", `calls` = List(1, 1)),
          GENOTYPES(`sampleId` = "2", `calls` = List(1, 0)),
          GENOTYPES(`sampleId` = "3", `calls` = List(0, 0)),
          GENOTYPES(`sampleId` = "4", `calls` = List(-1, -1))
        )
    )).toDF(),
    group.id -> groupDf,
    task.id -> taskDf,
    service_request.id -> serviceRequestDf
  )

  "variants job" should "transform data in expected format" in {

    val resultDf = job1.transform(data)
    resultDf.show(false)
    val result = resultDf.as[VariantRawOutput].collect().head

    result shouldBe VariantRawOutput(
      `created_on` = result.`created_on`)
  }
/*
  "variants job" should "scd1 using locus as key" in {

    val firstLoad = Seq(VCFInput("chr1"), VCFInput("chr2")).toDF()
    val secondLoad = Seq(VCFInput("chr1"), VCFInput("chr3")).toDF()
    val date1 = LocalDateTime.of(2021, 1, 1, 1, 1, 1)
    val date2 = LocalDateTime.of(2021, 1, 2, 1, 1, 1)

    val job1Df = job1.transform(Map(raw_variant_calling.id -> firstLoad), currentRunDateTime = date1)
    job1.load(job1Df)

    val job2Df = job2.transform(Map(raw_variant_calling.id -> secondLoad), currentRunDateTime = date2)
    job2Df.show(false)
    job2.load(job2Df)
    val resultDf = job2.destination.read
    resultDf.show(false)

    resultDf.as[VariantRawOutput].collect() should contain allElementsOf Seq(
      VariantRawOutput("1", `created_on` = Timestamp.valueOf(date1), `updated_on` = Timestamp.valueOf(date2), `batch_id` = "BAT2", `normalized_variants_oid` = Timestamp.valueOf(date2), `locus` = "1-69897-T-C"),
      VariantRawOutput("2", `created_on` = Timestamp.valueOf(date1), `updated_on` = Timestamp.valueOf(date1), `batch_id` = "BAT1", `normalized_variants_oid` = Timestamp.valueOf(date1), `locus` = "2-69897-T-C"),
      VariantRawOutput("3", `created_on` = Timestamp.valueOf(date2), `updated_on` = Timestamp.valueOf(date2), `batch_id` = "BAT2", `normalized_variants_oid` = Timestamp.valueOf(date2), `locus` = "3-69897-T-C")
    )
  }

 */
}
