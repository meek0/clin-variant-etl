package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.{WithSparkSession, WithTestConfig}
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.utils.ClassGenerator
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate

class CNVSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers {

  import spark.implicits._

  val raw_cnv: DatasetConf = conf.getDataset("raw_cnv")
  val specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val observation: DatasetConf = conf.getDataset("normalized_observation")
  val patient: DatasetConf = conf.getDataset("normalized_patient")
  val task: DatasetConf = conf.getDataset("normalized_task")

  val specimenDf: DataFrame = Seq(
    SpecimenOutput(`patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = Some("SA_001"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = None, `specimen_id` = Some("SP_001")),
    SpecimenOutput(`patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = Some("SA_002"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = None, `specimen_id` = Some("SP_002")),
    SpecimenOutput(`patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = Some("SA_003"), `specimen_id` = None),
    SpecimenOutput(`patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = None, `specimen_id` = Some("SP_003")),
  ).toDF

  val serviceRequestDf: DataFrame = Seq(
    ServiceRequestOutput(service_request_type = "analysis", `id` = "SRA0001", `patient_id` = "PA0001",
      family = Some(FAMILY(mother = Some("PA0003"), father = Some("PA0002"))),
      family_id = Some("FM00001"),
      `clinical_impressions` = Some(Seq("CI0001", "CI0002", "CI0003")),
      `service_request_description` = Some("Maladies musculaires (Panel global)")
    ),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0001", `patient_id` = "PA0001", analysis_service_request_id = Some("SRA0001"), `service_request_description` = Some("Maladies musculaires (Panel global)")),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0002", `patient_id` = "PA0002", analysis_service_request_id = Some("SRA0001"), `service_request_description` = Some("Maladies musculaires (Panel global)")),
    ServiceRequestOutput(service_request_type = "sequencing", `id` = "SRS0003", `patient_id` = "PA0003", analysis_service_request_id = Some("SRA0001"), `service_request_description` = Some("Maladies musculaires (Panel global)"))
  ).toDF()

  val clinicalImpressionsDf: DataFrame = Seq(
    ClinicalImpressionOutput(id = "CI0001", `patient_id` = "PA0001", observations = List("OB0001", "OB0099")),
    ClinicalImpressionOutput(id = "CI0002", `patient_id` = "PA0002", observations = List("OB0002")),
    ClinicalImpressionOutput(id = "CI0003", `patient_id` = "PA0003", observations = List("OB0003"))
  ).toDF()

  val observationsDf: DataFrame = Seq(
    ObservationOutput(id = "OB0001", patient_id = "PA0001", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    ObservationOutput(id = "OB0099", patient_id = "PA0001", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    ObservationOutput(id = "OB0002", patient_id = "PA0002", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    ObservationOutput(id = "OB0003", patient_id = "PA0003", `observation_code` = "DSTA", `interpretation_code` = "affected"),
  ).toDF()

  val patientDf: DataFrame = Seq(
    PatientOutput(`id` = "PA0001", `gender` = "male", `practitioner_role_id` = "PPR00101", `organization_id` = Some("OR00201")),
    PatientOutput(`id` = "PA0002", `gender` = "male", `practitioner_role_id` = "PPR00101", `organization_id` = Some("OR00201")),
    PatientOutput(`id` = "PA0003", `gender` = "female", `practitioner_role_id` = "PPR00101", `organization_id` = Some("OR00201")),
  ).toDF()

  val taskDf: DataFrame = Seq(
    TaskOutput(
      `id` = "73254",
      `patient_id` = "PA0001",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "11111"),
      `service_request_id` = "SRS0001"
    ),
    TaskOutput(
      `id` = "73256",
      `patient_id` = "PA0002",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "22222"),
      `service_request_id` = "SRS0002"
    ),
    TaskOutput(
      `id` = "73257",
      `patient_id` = "PA0003",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "33333"),
      `service_request_id` = "SRS0003"
    ),
    TaskOutput(
      `id` = "73255",
      `patient_id` = "PA00095",
      `specimen_id` = "TCGA-02-0001-01B-02D-0182-06",
      `experiment` = EXPERIMENT(`name` = "BAT1", `sequencing_strategy` = "WXS", `aliquot_id` = "11111"),
      `service_request_id` = "SRS0099"
    )
  ).toDF

  val data: Map[String, DataFrame] = Map(
    raw_cnv.id -> Seq(VCF_CNV_Input()).toDF(),
    specimen.id -> specimenDf,
    service_request.id -> serviceRequestDf,
    clinical_impression.id -> clinicalImpressionsDf,
    observation.id -> observationsDf,
    patient.id -> patientDf,
    task.id -> taskDf,
  )

  /*"occurrences transform" should "create the VCF_CNV_Input model" in {
    val cnv = spark.read.format("vcf").load("src/test/resources/test_json/cnv.vcf");
    ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "VCF_CNV_Input", cnv, "src/test/scala/")
  }*/

  "cnv transform" should "transform data in expected format" in {
    val results = new CNV("BAT1").transformSingle(data)
    val result = results.as[NormalizedCNV].collect()

    result should contain theSameElementsAs Seq(
      NormalizedCNV(),
    )

    // ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedCNV", result, "src/test/scala/")
  }

}