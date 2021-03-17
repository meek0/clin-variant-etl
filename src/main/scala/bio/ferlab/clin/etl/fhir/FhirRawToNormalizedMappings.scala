package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirCatalog.{Normalized, Raw}
import bio.ferlab.clin.etl.fhir.FhirCustomOperations._
import bio.ferlab.datalake.core.etl.DataSource
import bio.ferlab.datalake.core.transformation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

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
    Date("yyyy-MM-dd", "date"),
    Custom(
      _
        .withColumn("patient_id", patient_id)
        .withColumn("practitioner_id", practitioner_id)
        .withExtention("age_in_days", "extension.valueAge.value", "%/age-at-event", LongType)
    ),
    Drop("assessor", "subject", "extension")
  )

  val groupMappings: List[Transformation] = List(
    Custom(
      _
        .withColumn("members", transform(col("member"), c => regexp_replace(c("entity")("reference"), "Patient/", "")))
        .withExtention("family_structure_code", "extension.valueCoding.code", "%/fm-structure")
    ),
    Drop("member", "extension")
  )

  val observationMappings: List[Transformation] = List(
    Custom(
      _
        .withObservationExtension
        .withColumn("observation_description", col("code.coding.display")(0))
        .withColumn("observation_code", col("code.coding.code")(0))
        .withColumn("patient_id", patient_id)
        .withColumn("concept_code", col("valueCodeableConcept.coding.code")(0))
        .withColumn("concept_description", col("valueCodeableConcept.coding.display")(0))
        .withColumn("interpretation", col("interpretation")(0))
        .withColumn("interpretation_code", col("interpretation.coding.code")(0))
        .withColumn("interpretation_description_EN", col("interpretation.coding.display")(0))
        .withColumn("interpretation_description_FR", col("interpretation.text"))
        .withColumn("note", transform(col("note"), c => c("text")))
        .withColumn("category_description", col("category")(0)("coding")(0)("display"))
    ),
    Drop("extension", "code", "interpretation", "valueCodeableConcept", "subject", "category")
  )

  val organizationMappings: List[Transformation] = List(
    Custom(
      _
        .withColumn("code", col("type")(0)("coding")(0)("code"))
        .withColumn("description", col("type")(0)("coding")(0)("display"))
    ),
    Drop("type", "coding")
  )

  val patientMappings: List[Transformation]  = List(
    Custom(_.withColumnRenamed("birthDate", "birth_date")),
    Date("yyyy-MM-dd", "birth_date"),
    Custom (
      _
        .withColumn("practitioner_id", regexp_replace(col("generalPractitioner.reference")(0), "PractitionerRole/", ""))
        .withColumn("organization_id", regexp_replace(col("managingOrganization.reference"), "Organization/", ""))
        .withPatientNames
        .withPatientExtension
        .extractIdentifier(List("MR" -> "medical_record_number", "JHN" -> "jurisdictional_health_number"))
    ),
    Drop("name", "text", "extension", "managingOrganization", "generalPractitioner", "identifier")
  )

  val practitionerMappings: List[Transformation]  = List(
    Custom(
      _
        .withColumn("first_name", col("name")(0)("given")(0))
        .withColumn("last_name", col("name")(0)("family"))
        .withColumn("name_prefix", col("name")(0)("prefix")(0))
        .withColumn("name_suffix", trim(col("name")(0)("suffix")(0)))
        .withColumn("name_suffix", when(col("name_suffix") === "null", lit("")).otherwise(col("name_suffix")))
        .withColumn("full_name", trim(concat_ws(" ", col("name_prefix"), col("first_name"), col("last_name"), col("name_suffix"))))
        .withColumn("medical_license_number", col("identifier.value")(0))
    ),
    Drop("name", "identifier")
  )

  val practitionerRoleMappings: List[Transformation]  = List(
    Custom(
      _
        .withColumn("practitioner_id", regexp_replace(col("practitioner.reference"), "Practitioner/", ""))
        .withColumn("organization_id", organization_id)
        .withColumn("role_code", col("code")(0)("coding")(0)("code"))
        .withColumn("role_description_EN", col("code")(0)("coding")(0)("display"))
        .withColumn("role_description_FR", col("code")(0)("text"))
        .withTelecoms
    ),
    Drop("meta", "telecoms", "code", "practitioner", "organization")
  )

  val serviceRequestMappings: List[Transformation]  = List(
    Custom(_.withColumnRenamed("authoredOn", "authored_on")),
    Date("yyyy-MM-dd", "authored_on"),
    Custom(
      _
        .withColumn("category", col("category")(0)("text"))
        .withColumn("service_request_code", col("code.coding.code")(0))
        .withColumn("service_request_description", col("code.coding.display")(0))
        .withColumn("patient_id", patient_id)
        .withColumn("practitioner_id", regexp_replace(col("requester.reference"), "Practitioner/", ""))
        .withServiceRequestExtension
        .extractIdentifier(List("MR" -> "medical_record_number"))
        .withColumn("note", transform(col("note"), c =>
          struct(
            c("text").as("text"),
            to_timestamp(c("time"), "yyyy-MM-dd\'T\'HH:mm:ss.SSSz").as("time"),
            regexp_replace(c("authorReference")("reference"), "PractitionerRole/", "").as("practitioner_role_id")
          )))
    ),
    Drop("meta", "code", "subject", "requester", "extension", "identifier")
  )

  val mappings: List[(DataSource, DataSource, List[Transformation])] = List(
    (Raw.clinicalImpression, Normalized.clinicalImpression, defaultTransformations ++ clinicalImpressionMappings),
    (Raw.group             , Normalized.group             , defaultTransformations ++ groupMappings),
    (Raw.observation       , Normalized.observation       , defaultTransformations ++ observationMappings),
    (Raw.organization      , Normalized.organization      , defaultTransformations ++ organizationMappings),
    (Raw.patient           , Normalized.patient           , defaultTransformations ++ patientMappings),
    (Raw.practitioner      , Normalized.practitioner      , defaultTransformations ++ practitionerMappings),
    (Raw.practitionerRole  , Normalized.practitionerRole  , defaultTransformations ++ practitionerRoleMappings),
    (Raw.serviceRequest    , Normalized.serviceRequest    , defaultTransformations ++ serviceRequestMappings)
  )
}
