package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirCatalog.{Normalized, Raw}
import bio.ferlab.clin.etl.fhir.FhirCustomOperations._
import bio.ferlab.datalake.core.etl.DataSource
import bio.ferlab.datalake.core.transformation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object FhirRawToNormalizedMappings {
  val INPUT_FILENAME = "ingestionFileName"
  val INGESTION_TIMESTAMP = "ingestedOn"

  val defaultTransformations: List[Transformation]  = List(
    InputFileName(INPUT_FILENAME),
    InputFileTimestamp(INGESTION_TIMESTAMP),
    KeepFirstWithinPartition(Seq("id"), col(INGESTION_TIMESTAMP).desc_nulls_last),
    Custom(_.withMetadata),
    Drop("meta")
  )

  val clinicalImpressionMappings: List[Transformation] = List(
    Date("yyyy-MM-dd", "date"),
    Custom(
      _
        .withColumn("patientId", patientId)
        .withColumn("practitionerId", practitionerId)
        .withExtention("ageInDays", "extension.valueAge.value", "%/age-at-event", LongType)
    ),
    Drop("assessor", "subject", "extension")
  )

  val groupMappings: List[Transformation] = List(
    Custom(
      _
        .withColumn("members", transform(col("member"), c => regexp_replace(c("entity")("reference"), "Patient/", "")))
        .withExtention("familyStructureCode", "extension.valueCoding.code", "%/fm-structure")
    ),
    Drop("member", "extension")
  )

  val observationMappings: List[Transformation] = List(
    Custom(
      _
        .withObservationExtension
        .withColumn("observationDescription", col("code.coding.display")(0))
        .withColumn("observationCode", col("code.coding.code")(0))
        .withColumn("patientId", patientId)
        .withColumn("conceptCode", col("valueCodeableConcept.coding.code")(0))
        .withColumn("conceptDescription", col("valueCodeableConcept.coding.display")(0))
        .withColumn("interpretation", col("interpretation")(0))
        .withColumn("interpretationCode", col("interpretation.coding.code")(0))
        .withColumn("interpretationDescriptionEN", col("interpretation.coding.display")(0))
        .withColumn("interpretationDescriptionFR", col("interpretation.text"))
        .withColumn("note", transform(col("note"), c => c("text")))
        .withColumn("categoryDescription", col("category")(0)("coding")(0)("display"))
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
    Date("yyyy-MM-dd", "birthDate"),
    Custom (
      _
        .withColumn("practitionerId", regexp_replace(col("generalPractitioner.reference")(0), "PractitionerRole/", ""))
        .withColumn("organizationId", regexp_replace(col("managingOrganization.reference"), "Organization/", ""))
        .withPatientNames
        .withPatientExtension
        .extractIdentifier(List("MR" -> "medical_record_number", "JHN" -> "jurisdictional_health_number"))
    ),
    Drop("name", "text", "extension", "managingOrganization", "generalPractitioner", "identifier")
  )

  val practitionerMappings: List[Transformation]  = List(
    Custom(
      _
        .withColumn("firstName", col("name")(0)("given")(0))
        .withColumn("lastName", col("name")(0)("family"))
        .withColumn("namePrefix", col("name")(0)("prefix")(0))
        .withColumn("nameSuffix", trim(col("name")(0)("suffix")(0)))
        .withColumn("nameSuffix", when(col("nameSuffix") === "null", lit("")).otherwise(col("nameSuffix")))
        .withColumn("fullName", trim(concat_ws(" ", col("namePrefix"), col("firstName"), col("lastName"), col("nameSuffix"))))
        .withColumn("medicalLicenseNumber", col("identifier.value")(0))
    ),
    Drop("name", "identifier")
  )

  val practitionerRoleMappings: List[Transformation]  = List(
    Custom(
      _
        .withColumn("practitionerId", regexp_replace(col("practitioner.reference"), "Practitioner/", ""))
        .withColumn("organizationId", organizationId)
        .withColumn("roleCode", col("code")(0)("coding")(0)("code"))
        .withColumn("roleDescriptionEN", col("code")(0)("coding")(0)("display"))
        .withColumn("roleDescriptionFR", col("code")(0)("text"))
        .withTelecoms
    ),
    Drop("meta", "telecoms", "code", "practitioner", "organization")
  )

  val serviceRequestMappings: List[Transformation]  = List(
    Date("yyyy-MM-dd", "authoredOn"),
    Custom(
      _
        .withColumn("category", col("category")(0)("text"))
        .withColumn("serviceRequestCode", col("code.coding.code")(0))
        .withColumn("serviceRequestDescription", col("code.coding.display")(0))
        .withColumn("patientId", patientId)
        .withColumn("practitionerId", regexp_replace(col("requester.reference"), "Practitioner/", ""))
        .withServiceRequestExtension
        .extractIdentifier(List("MR" -> "medical_record_number"))
        .withColumn("note", transform(col("note"), c =>
          struct(
            c("text").as("text"),
            to_timestamp(c("time"), "yyyy-MM-dd\'T\'HH:mm:ss.SSSz").as("time"),
            regexp_replace(c("authorReference")("reference"), "PractitionerRole/", "").as("practitionerRoleId")
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
