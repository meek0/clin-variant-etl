package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model.{BiospecimenOutput, FamilyRelationshipOutput, OccurrenceRawOutput, VCFInput}
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
  val biospecimens: DatasetConf = conf.getDataset("biospecimens")
  val family_relationships: DatasetConf = conf.getDataset("family_relationships")

  val patientDf = Seq(
    PatientOutput(
      `patient_id` = "PA0001",
      `family_id` = "FM00001",
      `gender` = "male"
    ),
    PatientOutput(
      `patient_id` = "PA0002",
      `family_id` = "FM00001",
      `gender` = "male"
    ),
    PatientOutput(
      `patient_id` = "PA0003",
      `family_id` = "FM00001",
      `gender` = "female"
    )
  ).toDF()

  val biospecimenDf = Seq(
    BiospecimenOutput(
      `biospecimen_id`= "SP14909",
      `family_id` = "FM00001",
      `patient_id` = "PA0001"
    )
  ).toDF

  val familyRelationshipsDf = Seq(
    FamilyRelationshipOutput(
      `patient1` = "PA0001",
      `patient2` = "PA0002",
      `patient1_to_patient2_relation` = "Child",
      `patient2_to_patient1_relation` = "Father",
    ),
    FamilyRelationshipOutput(
      `patient1` = "PA0001",
      `patient2` = "PA0003",
      `patient1_to_patient2_relation` = "Child",
      `patient2_to_patient1_relation` = "Mother",
    ),
    FamilyRelationshipOutput(
      `patient1` = "PA0002",
      `patient2` = "PA0001",
      `patient1_to_patient2_relation` = "Father",
      `patient2_to_patient1_relation` = "Child",
    ),
    FamilyRelationshipOutput(
      `patient1` = "PA0003",
      `patient2` = "PA0001",
      `patient1_to_patient2_relation` = "Mother",
      `patient2_to_patient1_relation` = "Child",
    )
  ).toDF

  val data = Map(
    complete_joint_calling.id -> Seq(VCFInput()).toDF(),
    patient.id -> patientDf,
    biospecimens.id -> biospecimenDf,
    family_relationships.id -> familyRelationshipsDf
  )


  "occurrences transform" should "transform data in expected format" in {
    val result = new Occurrences("BAT1").transform(data)
    result.as[OccurrenceRawOutput].collect() should contain allElementsOf Seq(
      OccurrenceRawOutput(`last_update` = Date.valueOf(LocalDate.now()))
    )
  }

}
//TODO replace with bio.ferlab.clin.model.PatientOutput
case class PatientOutput(`patient_id`: String = "PA0001",
                         `family_id`: String = "FA0001",
                         `practitioner_id`: String = "PPR00101",
                         `organization_id`: String = "OR00201",
                         `study_id`: String = "ET00010",
                         `is_proband`: Boolean = true,
                         `affected_status`: String = "true",
                         `gender`: String = "male",
                         `is_fetus`: Boolean = false)
