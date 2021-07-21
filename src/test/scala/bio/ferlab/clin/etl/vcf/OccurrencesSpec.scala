package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model.{BiospecimenOutput, OccurrenceRawOutput, VCFInput}
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, StorageConf}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate

class OccurrencesSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  implicit val localConf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_storage", this.getClass.getClassLoader.getResource(".").getFile)))

  import spark.implicits._

  val data = Map(
    "complete_joint_calling" -> Seq(VCFInput()).toDF(),
    "patient" -> Seq(PatientOutput()).toDF,
    "biospecimens" -> Seq(BiospecimenOutput()).toDF
  )


  "occurrences transform" should "transform data in expected format" in {
    new Occurrences("BAT1").transform(data).as[OccurrenceRawOutput].collect() should contain allElementsOf Seq(
      OccurrenceRawOutput(`last_update` = Date.valueOf(LocalDate.now()))
    )
  }

}
//TODO replace with bio.ferlab.clin.model.PatientOutput
case class PatientOutput(`patient_id`: String = "PA0001",
                         `family_id`: String = "FA0001",
                         `practitioner_id`: String = "PPR00101",
                         `organization_id`: String = "OR00201",
                         `study_id`: String = "ET00010")
