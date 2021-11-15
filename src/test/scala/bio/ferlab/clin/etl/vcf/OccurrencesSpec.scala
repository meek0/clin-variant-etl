package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate

class OccurrencesSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  import spark.implicits._

  val raw_variant_calling: DatasetConf = conf.getDataset("raw_variant_calling")
  val patient: DatasetConf = conf.getDataset("normalized_patient")
  val specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val group: DatasetConf = conf.getDataset("normalized_group")
  val task: DatasetConf = conf.getDataset("normalized_task")

  val patientDf = Seq(
    PatientOutput(
      `id` = "PA0001",
      `family_id` = "FM00001",
      `gender` = "male",
      `practitioner_id` = "PPR00101",
      `organization_id` = "OR00201",
      `family_relationship` = List()
    ),
    PatientOutput(
      `id` = "PA0002",
      `family_id` = "FM00001",
      `gender` = "male",
      `family_relationship` = List(FAMILY_RELATIONSHIP("PA0001", "FTH"))
    ),
    PatientOutput(
      `id` = "PA0003",
      `family_id` = "FM00001",
      `gender` = "female",
      `family_relationship` = List(FAMILY_RELATIONSHIP("PA0001", "MTH"))
    )
  ).toDF()

  val specimenDf = Seq(
    SpecimenOutput(
      `aliquot_id` = "TCGA-02-0001-01B-02D-0182-06",
      `patient_id` = "PA0001"
    )
  ).toDF

  val groupDf = Seq(
    GroupOutput(
      `id` = "FM00001",
      `members` = List(
        MEMBERS("PA0001", `affected_status` = true),
        MEMBERS("PA0002", `affected_status` = true),
        MEMBERS("PA0003", `affected_status` = true)
      )
    )
  ).toDF()

  val taskDf = Seq(
    TaskOutput(
      `id` = "73254",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `document_id` = "1",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS")
    ),
    TaskOutput(
      `id` = "73255",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `document_id` = "2",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS")
    )
  ).toDF

  val data = Map(
    raw_variant_calling.id -> Seq(VCFInput()).toDF(),
    patient.id -> patientDf,
    specimen.id -> specimenDf,
    group.id -> groupDf,
    task.id -> taskDf
  )


  "occurrences transform" should "transform data in expected format" in {
    val result = new Occurrences("BAT1", "chr1").transform(data)
    result.as[OccurrenceRawOutput].collect() should contain allElementsOf Seq(
      OccurrenceRawOutput(`last_update` = Date.valueOf(LocalDate.now()))
    )
  }
}
