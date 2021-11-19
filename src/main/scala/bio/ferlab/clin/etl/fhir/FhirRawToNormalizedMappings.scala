package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirCustomOperations._
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.transformation._
import org.apache.spark.sql.functions._

object FhirRawToNormalizedMappings {
  val INPUT_FILENAME = "ingestion_file_name"
  val INGESTION_TIMESTAMP = "ingested_on"

  val defaultTransformations: List[Transformation]  = List(
    InputFileName(INPUT_FILENAME),
    InputFileTimestamp(INGESTION_TIMESTAMP),
    KeepFirstWithinPartition(Seq("id"), col(INGESTION_TIMESTAMP).desc_nulls_last),
    Custom(_
      .withColumnRenamed("resourceType", "resource_type")
      .withMetadata),
    Drop("meta")
  )

  val clinicalImpressionMappings: List[Transformation] = List(
    ToDate("yyyy-MM-dd", "date"),
    Custom(
      _
        //.withColumnRenamed("id", "clinical_impression_id")
        .withColumn("patient_id", patient_id)
        .withColumn("practitioner_id", practitioner_id)
        .withColumn("age_at_event_in_days", col("extension")(0)("valueAge")("value"))
    ),
    Drop("assessor", "subject", "extension")
  )

  val groupMappings: List[Transformation] = List(
    Custom(
      _
        //.withColumnRenamed("id", "group_id")
        .withColumn("members", transform(col("member"), c =>
          struct(
            regexp_replace(c("entity")("reference"), "Patient/", "") as "patient_id",
            when(c("extension")(0)("valueCoding")("display") === "Affected", lit(true)).otherwise(lit(false)) as "affected_status"
          ))
        )
        .withColumn("family_structure_code", col("extension")(0)("valueCoding")("code"))
    ),
    Drop("member", "extension")
  )

  val observationMappings: List[Transformation] = List(
    Custom(
      _
        //.withColumnRenamed("id", "observation_id")
        .withObservationExtension
        .withColumn("observation_code", col("code.coding.code")(0))
        .withColumn("patient_id", patient_id)
        .withColumn("concept_code", col("valueCodeableConcept.coding.code")(0))
        .withColumn("concept_description", col("valueCodeableConcept.coding.display")(0))
        .withColumn("interpretation", col("interpretation")(0))
        .withColumn("interpretation_code", col("interpretation.coding.code")(0))
        .withColumn("interpretation_description", col("interpretation.coding.display")(0))
        .withColumn("note", transform(col("note"), c => c("text")))
        .withColumn("category_description", col("category")(0)("coding")(0)("display"))
    ),
    Drop("extension", "code", "interpretation", "valueCodeableConcept", "subject", "category", "valueBoolean", "valueString")
  )

  val organizationMappings: List[Transformation] = List(
    Custom(
      _
        //.withColumnRenamed("id", "organization_id")
        .withColumn("code", col("type")(0)("coding")(0)("code"))
        .withColumn("description", col("type")(0)("coding")(0)("display"))
    ),
    Drop("type", "coding")
  )

  val patientMappings: List[Transformation]  = List(
    Custom(_.withColumnRenamed("birthDate", "birth_date")),
    ToDate("yyyy-MM-dd", "birth_date"),
    Custom (
      _
        .withColumn("organization_id", regexp_replace(col("identifier")(0)("assigner")("reference"), "Organization/", ""))
        .extractIdentifier(List("MR" -> "medical_record_number", "JHN" -> "jurisdictional_health_number"))
        .withColumn("practitioner_role_id", regexp_replace(col("generalPractitioner.reference")(0), "PractitionerRole/", ""))
        .withPatientExtension
    ),
    Drop("name", "text", "extension", "generalPractitioner", "identifier")
  )

  val practitionerMappings: List[Transformation]  = List(
    Custom(
      _
        //.withColumnRenamed("id", "practitioner_id")
        .withColumn("first_name", col("name")(0)("given")(0))
        .withColumn("last_name", col("name")(0)("family"))
        .withColumn("name_prefix", col("name")(0)("prefix")(0))
        //.withColumn("name_suffix", trim(col("name")(0)("suffix")(0)))
        //.withColumn("name_suffix", when(col("name_suffix") === "null", lit("")).otherwise(col("name_suffix")))
        .withColumn("full_name", trim(concat_ws(" ", col("name_prefix"), col("first_name"), col("last_name"))))
        .withColumn("medical_license_number", col("identifier.value")(0))
    ),
    Drop("name", "identifier")
  )

  val practitionerRoleMappings: List[Transformation]  = List(
    Custom(
      _
        .withTelecoms
        .withColumn("practitioner_id", regexp_replace(col("practitioner.reference"), "Practitioner/", ""))
        .withColumn("organization_id", organization_id)
        .withColumn("role_code", col("code")(0)("coding")(0)("code"))
        .withColumn("role_description", col("code")(0)("coding")(0)("display"))
        //.withColumn("role_description_FR", col("code")(0)("text"))
        //.withColumnRenamed("id", "practitioner_role_id")

    ),
    Drop("meta", "telecoms", "code", "practitioner", "organization")
  )

  val serviceRequestMappings: List[Transformation]  = List(
    Custom(_.withColumnRenamed("authoredOn", "authored_on")),
    ToDate("yyyy-MM-dd", "authored_on"),
    Custom(
      _
        //.extractIdentifier(List("MR" -> "medical_record_number"))
        .withColumn("specimens", transform(col("specimen"), c => regexp_replace(c("reference"), "Specimen/", "")))
        .withColumn("service_request_code", col("code.coding.code")(0))
        .withColumn("service_request_description", col("code.coding.display")(0))
        .withColumn("patient_id", patient_id)
        .withColumn("practitioner_id", regexp_replace(col("requester.reference"), "Practitioner/", ""))
        .withServiceRequestExtension
        .withColumn("note", transform(col("note"), c =>
          struct(
            c("text").as("text"),
            to_timestamp(c("time"), "yyyy-MM-dd\'T\'HH:mm:ss.SSSz").as("time"),
            regexp_replace(c("authorReference")("reference"), "Practitioner/", "").as("practitioner_id")
          )))
    ),
    Drop("meta", "code", "subject", "requester", "extension", "specimen")
  )

  val specimenMapping: List[Transformation]  = List(
    Custom(_
      .withColumn("parent_id", regexp_replace(col("parent.reference")(0), "Specimen/", ""))
      .withColumn("organization_id", regexp_replace(col("accessionIdentifier.assigner.reference"), "Organization/", ""))
      .withColumn("specimen_id", when(col("accessionIdentifier.system").like("%specimen"), col("accessionIdentifier.value")))
      .withColumn("sample_id", when(col("accessionIdentifier.system").like("%sample"), col("accessionIdentifier.value")))
      .withColumn("aliquot_id", when(col("accessionIdentifier.system").like("%aliquot"), col("accessionIdentifier.value")))
      .withColumn("patient_id", regexp_replace(col("subject.reference"), "Patient/", ""))
      .withColumn("service_request_id", regexp_replace(col("request.reference")(0), "ServiceRequest/", ""))
      .withColumn("received_time", to_timestamp(col("receivedTime"), "yyyy-MM-dd\'T\'HH:mm:sszzzz"))
      .withColumn("specimen_type", col("type")("coding")(0)("code"))
    ),
    Drop("meta", "parent", "subject", "receivedTime", "accessionIdentifier", "request", "type")
  )

  val taskMapping: List[Transformation]  = List(
    Custom(_
      .withColumn("organization_id", regexp_replace(col("owner.reference"), "Organization/", ""))
      .withColumn("specimen_id", regexp_replace(col("input")(0)("valueReference")("reference"), "Specimen/", ""))
      .withColumn("document_id", regexp_replace(col("output")(0)("valueReference")("reference"), "DocumentReference/", ""))
      .withColumn("patient_id", regexp_replace(col("for")("reference"), "Patient/", ""))
      .withColumn("service_request_id", regexp_replace(col("focus")("reference"), "ServiceRequest/", ""))
      .withColumn("code", col("code")("coding")(0)("code"))
      .withColumn("authored_on", to_timestamp(col("authoredOn"), "yyyy-MM-dd\'T\'HH:mm:sszzzz"))
      .withTaskExtension
    ),
    Drop("meta", "owner", "authoredOn", "extension", "input", "output", "focus", "for")
  )

  def mappings(implicit c: Configuration): List[(DatasetConf, DatasetConf, List[Transformation])] = List(
    (c.getDataset("raw_clinical_impression"), c.getDataset("normalized_clinical_impression"), defaultTransformations ++ clinicalImpressionMappings),
    (c.getDataset("raw_group")              , c.getDataset("normalized_group")              , defaultTransformations ++ groupMappings),
    (c.getDataset("raw_observation")        , c.getDataset("normalized_observation")        , defaultTransformations ++ observationMappings),
    (c.getDataset("raw_organization")       , c.getDataset("normalized_organization")       , defaultTransformations ++ organizationMappings),
    (c.getDataset("raw_patient")            , c.getDataset("normalized_patient")            , defaultTransformations ++ patientMappings),
    (c.getDataset("raw_practitioner")       , c.getDataset("normalized_practitioner")       , defaultTransformations ++ practitionerMappings),
    (c.getDataset("raw_practitioner_role")  , c.getDataset("normalized_practitioner_role")  , defaultTransformations ++ practitionerRoleMappings),
    (c.getDataset("raw_service_request")    , c.getDataset("normalized_service_request")    , defaultTransformations ++ serviceRequestMappings),
    (c.getDataset("raw_specimen")           , c.getDataset("normalized_specimen")           , defaultTransformations ++ specimenMapping),
    (c.getDataset("raw_task")               , c.getDataset("normalized_task")               , defaultTransformations ++ taskMapping)
  )
}
