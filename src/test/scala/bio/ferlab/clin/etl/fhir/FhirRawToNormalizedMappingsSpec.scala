package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirToNormalizedETL.getSchema
import bio.ferlab.clin.model._
import bio.ferlab.clin.testutils.WithSparkSession
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FhirRawToNormalizedMappingsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/test.conf")
    .copy(storages = List(StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)))

  import spark.implicits._

  "clinicalImpression raw job" should "return data in the expected format" in {
    val inputDs = conf.getDataset("raw_clinical_impression")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new FhirToNormalizedETL(src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_clinical_impression")).json("src/test/resources/raw/landing/fhir/ClinicalImpression/ClinicalImpression_0_19000101_000000.json")
    val output = job.transform(Map(inputDs.id -> inputDf))

    output.count() shouldBe 7
    val head = output.where(col("id") === "CI0005").as[ClinicalImpressionOutput].head()
    head shouldBe ClinicalImpressionOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "observation raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_observation")

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new FhirToNormalizedETL(src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_observation")).json("src/test/resources/raw/landing/fhir/Observation/Observation_0_19000101_000000.json")
    val output = job.transform(Map(inputDs.id -> inputDf))

    output.count() shouldBe 4
    val head = output.where("id='OB00001'").as[ObservationOutput].head()
    head shouldBe ObservationOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "organization raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_organization")

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new FhirToNormalizedETL(src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_organization")).json("src/test/resources/raw/landing/fhir/Organization/Organization_0_19000101_000000.json")
    val output = job.transform(Map(inputDs.id -> inputDf))

    output.count() shouldBe 8
    val head = output.where("id='CHUSJ'").as[OrganizationOutput].head()
    head shouldBe OrganizationOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)
  }

  "patient raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_patient")

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new FhirToNormalizedETL(src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_patient")).json("src/test/resources/raw/landing/fhir/Patient/Patient_1_19000101_102715.json")
    val output = job.transform(Map(inputDs.id -> inputDf))

    output.count() shouldBe 3
    val head = output.where("id='PA00004'").as[PatientOutput].head()
    head shouldBe PatientOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)
  }

  "practitioner raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_practitioner")

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new FhirToNormalizedETL(src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_practitioner")).json("src/test/resources/raw/landing/fhir/Practitioner/Practitioner_0_19000101_000000.json")
    val output = job.transform(Map(inputDs.id -> inputDf))

    output.count() shouldBe 2
    val head = output.where("id='PR00101'").as[PractitionerOutput].head()
    head shouldBe PractitionerOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "practitioner role raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_practitioner_role")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new FhirToNormalizedETL(src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_practitioner_role")).json("src/test/resources/raw/landing/fhir/PractitionerRole/PractitionerRole_0_19000101_000000.json")
    val output = job.transform(Map(inputDs.id -> inputDf))

    output.count() shouldBe 2
    val head = output.where("id='PRR00101'").as[PractitionerRoleOutput].head()
    head shouldBe PractitionerRoleOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "service request raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_service_request")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = new FhirToNormalizedETL(src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_service_request")).json("src/test/resources/raw/landing/fhir/ServiceRequest/service_request_with_family.json")
    val result = job.transform(Map(inputDs.id -> inputDf))

    result.count() shouldBe 2
    val analysisServiceRequest = result.where("id='2908'").as[ServiceRequestOutput].head()
    analysisServiceRequest shouldBe ServiceRequestOutput()
      .copy(`ingestion_file_name` = analysisServiceRequest.`ingestion_file_name`, `ingested_on` = analysisServiceRequest.`ingested_on`,
        `updated_on` = analysisServiceRequest.`updated_on`, `created_on` = analysisServiceRequest.`created_on`, `note` = analysisServiceRequest.`note`)
    val sequencingServiceRequest = result.where("id='2916'").as[ServiceRequestOutput].head()
    sequencingServiceRequest shouldBe ServiceRequestOutput(id = "2916",
      `ingestion_file_name` = analysisServiceRequest.`ingestion_file_name`, `ingested_on` = analysisServiceRequest.`ingested_on`,
      `updated_on` = analysisServiceRequest.`updated_on`, `created_on` = analysisServiceRequest.`created_on`, `note` = analysisServiceRequest.`note`,
      `specimens` = Some(List("2923", "2924")), patient_id = "2919", family = None, `clinical_impressions` = None,
      service_request_type = "sequencing", analysis_service_request_id = Some("2908"), family_id = None)

  }

  "specimen raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_specimen")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val inputDf = spark.read.schema(getSchema("raw_specimen")).json("src/test/resources/raw/landing/fhir/Specimen/Specimen_0_19000101_000000.json")
    val job = new FhirToNormalizedETL(src, dst, mapping)

    val result = job.transform(Map(inputDs.id -> inputDf))

    result.count() shouldBe 7
    val head = result.where("id='134658'").as[SpecimenOutput].collect().head
    head shouldBe SpecimenOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`, `received_time` = head.`received_time`)

  }

  "task raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_task")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val inputDf = spark.read.schema(getSchema("raw_task")).json("src/test/resources/raw/landing/fhir/Task/Task_0_19000101_000000.json")
    val job = new FhirToNormalizedETL(src, dst, mapping)
    val result = job.transform(Map(inputDs.id -> inputDf)).where("id='109351'")

    result.count() shouldBe 1
    val head = result.as[TaskOutput].collect().head
    head shouldBe TaskOutput()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`, `authored_on` = head.`authored_on`, `experiment` = head.`experiment`)
  }

}
