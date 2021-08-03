package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate

class OccurrencesSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile)))

  import spark.implicits._

  val complete_joint_calling: DatasetConf = conf.getDataset("complete_joint_calling")
  val patient: DatasetConf = conf.getDataset("patient")
  val biospecimen: DatasetConf = conf.getDataset("biospecimen")
  val group: DatasetConf = conf.getDataset("group")

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

  val biospecimenDf = Seq(
    BiospecimenOutput(
      `biospecimen_id`= "SP14909",
      //`family_id` = "FM00001",
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

  val data = Map(
    complete_joint_calling.id -> Seq(VCFInput()).toDF(),
    patient.id -> patientDf,
    biospecimen.id -> biospecimenDf,
    group.id -> groupDf
  )


  "occurrences transform" should "transform data in expected format" in {
    val result = new Occurrences("BAT1").transform(data)
    result.as[OccurrenceRawOutput].collect() should contain allElementsOf Seq(
      OccurrenceRawOutput(`last_update` = Date.valueOf(LocalDate.now()))
    )
  }
}
