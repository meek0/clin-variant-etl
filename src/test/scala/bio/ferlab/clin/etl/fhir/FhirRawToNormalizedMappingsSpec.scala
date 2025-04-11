package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirToNormalizedETL.getSchema
import bio.ferlab.clin.model.normalized.fhir._
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{ClassGenerator, SparkSpec, TestETLContext}
import org.apache.spark.sql.functions._

class FhirRawToNormalizedMappingsSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  "clinicalImpression raw job" should "return data in the expected format" in {
    val inputDs = conf.getDataset("raw_clinical_impression")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_clinical_impression")).json("src/test/resources/raw/landing/fhir/ClinicalImpression/ClinicalImpression_0_19000101_000000.json")
    val output = job.transformSingle(Map(inputDs.id -> inputDf))

    output.count() shouldBe 7
    val head = output.where(col("id") === "CI0005").as[NormalizedClinicalImpression].head()
    head shouldBe NormalizedClinicalImpression()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "codeSystem raw job" should "return data in the expected format" in {
    val inputDs = conf.getDataset("raw_code_system")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_code_system")).json("src/test/resources/raw/landing/fhir/CodeSystem/CodeSystem_0_19000101_000000.json")
    val output = job.transformSingle(Map(inputDs.id -> inputDf))

    output.count() shouldBe 8
    val head = output.where(col("id") === "variant-type" && col("concept_code") === "S").as[NormalizedCodeSystem].head()
    head shouldBe NormalizedCodeSystem()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)
  }

  "observation raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_observation")

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_observation")).json("src/test/resources/raw/landing/fhir/Observation/Observation_0_19000101_000000.json")
    val output = job.transformSingle(Map(inputDs.id -> inputDf))

    output.count() shouldBe 4
    val head = output.where("id='OB00001'").as[NormalizedObservation].head()
    head shouldBe NormalizedObservation()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`, `concept_values` = None)
  }

  "organization raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_organization")

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_organization")).json("src/test/resources/raw/landing/fhir/Organization/Organization_0_19000101_000000.json")
    val output = job.transformSingle(Map(inputDs.id -> inputDf))

    output.count() shouldBe 8
    val head = output.where("id='CHUSJ'").as[NormalizedOrganization].head()
    head shouldBe NormalizedOrganization()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)
  }

  "patient raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_patient")

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_patient")).json("src/test/resources/raw/landing/fhir/Patient/Patient_1_19000101_102715.json")
    val output = job.transformSingle(Map(inputDs.id -> inputDf))

    output.count() shouldBe 3
    val head = output.where("id='PA00004'").as[NormalizedPatient].head()
    head shouldBe NormalizedPatient()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)
  }

  "practitioner raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_practitioner")

    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_practitioner")).json("src/test/resources/raw/landing/fhir/Practitioner/Practitioner_0_19000101_000000.json")
    val output = job.transformSingle(Map(inputDs.id -> inputDf))

    output.count() shouldBe 2
    val head = output.where("id='PR00101'").as[NormalizedPractitioner].head()
    head shouldBe NormalizedPractitioner()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "practitioner role raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_practitioner_role")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_practitioner_role")).json("src/test/resources/raw/landing/fhir/PractitionerRole/PractitionerRole_0_19000101_000000.json")
    val output = job.transformSingle(Map(inputDs.id -> inputDf))

    output.count() shouldBe 2
    val head = output.where("id='PRR00101'").as[NormalizedPractitionerRole].head()
    head shouldBe NormalizedPractitionerRole()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)

  }

  "service request raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_service_request")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_service_request")).json("src/test/resources/raw/landing/fhir/ServiceRequest/service_request_with_family.json")
    val result = job.transformSingle(Map(inputDs.id -> inputDf))

    result.count() shouldBe 2
    val analysisServiceRequest = result.where("id='2908'").as[NormalizedServiceRequest].head()
    analysisServiceRequest shouldBe NormalizedServiceRequest()
      .copy(`ingestion_file_name` = analysisServiceRequest.`ingestion_file_name`, `ingested_on` = analysisServiceRequest.`ingested_on`,
        `updated_on` = analysisServiceRequest.`updated_on`, `created_on` = analysisServiceRequest.`created_on`, `note` = analysisServiceRequest.`note`)
    val sequencingServiceRequest = result.where("id='2916'").as[NormalizedServiceRequest].head()
    sequencingServiceRequest shouldBe NormalizedServiceRequest(id = "2916",
      `ingestion_file_name` = analysisServiceRequest.`ingestion_file_name`, `ingested_on` = analysisServiceRequest.`ingested_on`,
      `updated_on` = analysisServiceRequest.`updated_on`, `created_on` = analysisServiceRequest.`created_on`, `note` = analysisServiceRequest.`note`,
      `specimens` = Some(List("2923", "2924")), patient_id = "2919", family = None, `clinical_impressions` = None,
      service_request_type = "sequencing", analysis_service_request_id = Some("2908"), family_id = None)

  }

  "family raw job" should "return data in the expected format" in {
    val inputDs = conf.getDataset("raw_service_request")
    val outputDs = conf.getDataset("normalized_family")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._2 == outputDs).get
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val inputDf = spark.read.schema(getSchema("raw_service_request")).json("src/test/resources/raw/landing/fhir/ServiceRequest/service_request_with_siblings.json")
    val result = job.transformSingle(Map(inputDs.id -> inputDf))

    result.count() shouldBe 7

    val proband = result.where("patient_id='2909'").as[NormalizedFamily].head()
    proband shouldBe NormalizedFamily()

    val brother = result.where("patient_id='2923'").as[NormalizedFamily].head()
    brother shouldBe NormalizedFamily(patient_id = "2923")

    val sister1 = result.where("patient_id='2921'").as[NormalizedFamily].head()
    sister1 shouldBe NormalizedFamily(patient_id = "2921")

    val sister2 = result.where("patient_id='2922'").as[NormalizedFamily].head()
    sister2 shouldBe NormalizedFamily(patient_id = "2922")

    val mother = result.where("patient_id='2919'").as[NormalizedFamily].head()
    mother shouldBe NormalizedFamily(patient_id = "2919", family = None)

    val father = result.where("patient_id='2920'").as[NormalizedFamily].head()
    father shouldBe NormalizedFamily(patient_id = "2920", family = None)

    val probandOnly = result.where("patient_id='3000'").as[NormalizedFamily].head()
    probandOnly shouldBe NormalizedFamily(patient_id = "3000", family_id = None, family = None, analysis_service_request_id = "2916")

  }

  "specimen raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_specimen")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val inputDf = spark.read.schema(getSchema("raw_specimen")).json("src/test/resources/raw/landing/fhir/Specimen/Specimen_0_19000101_000000.json")
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)

    val result = job.transformSingle(Map(inputDs.id -> inputDf))

    result.count() shouldBe 7
    val head = result.where("id='134658'").as[NormalizedSpecimen].collect().head
    head shouldBe NormalizedSpecimen()
      .copy(`ingestion_file_name` = head.`ingestion_file_name`, `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`, `received_time` = head.`received_time`)

  }

  "task raw job" should "return data in the expected format" in {

    val inputDs = conf.getDataset("raw_task")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val inputPath = getClass.getResource("/raw/landing/fhir/Task/Task_0_19000101_000000.json").getPath
    val inputDf = spark.read.schema(getSchema("raw_task")).json(inputPath)
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val result = job.transformSingle(Map(inputDs.id -> inputDf)).where("id='1'")

    result.count() shouldBe 1
    val head = result.as[NormalizedTask].collect().head
    head shouldBe NormalizedTask()
      .copy(`ingestion_file_name` = s"file://$inputPath", `ingested_on` = head.`ingested_on`,
        `updated_on` = head.`updated_on`, `created_on` = head.`created_on`, `authored_on` = head.`authored_on`)
  }

  "documentReference raw job" should "return data in the expected format" in {
    val inputDs = conf.getDataset("raw_document_reference")
    val (src, dst, mapping) = FhirRawToNormalizedMappings.mappings.find(_._1 == inputDs).get
    val inputPath = getClass.getResource("/raw/landing/fhir/DocumentReference/DocumentReference.json").getPath
    val inputDf = spark.read.schema(getSchema("raw_document_reference")).json(inputPath)
    val job = FhirToNormalizedETL(TestETLContext(), src, dst, mapping)
    val result = job.transformSingle(Map(inputDs.id -> inputDf))
    result.count() shouldBe 1
    val head = result.as[NormalizedDocumentReference].collect().head
    head shouldBe NormalizedDocumentReference()
      .copy(ingestion_file_name = s"file://$inputPath", `ingested_on` = head.`ingested_on`, `updated_on` = head.`updated_on`, `created_on` = head.`created_on`)
  }

}
