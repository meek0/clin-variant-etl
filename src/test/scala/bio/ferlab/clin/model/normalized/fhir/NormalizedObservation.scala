/**
 * Generated by [[bio.ferlab.datalake.testutils.ClassGenerator]]
 * on 2025-02-21T12:24:49.894422
 */
package bio.ferlab.clin.model.normalized.fhir

import java.sql.Timestamp


case class NormalizedObservation(`id`: String = "OB00001",
                                 `note`: Seq[String] = Seq("une précision sur le CGH anormal"),
                                 `resource_type`: String = "Observation",
                                 `status`: String = "final",
                                 `boolean_value`: Option[Boolean] = None,
                                 `string_value`: Option[String] = None,
                                 `ingestion_file_name`: String = "/raw/landing/fhir/Observation/Observation_0_19000101_000000.json",
                                 `ingested_on`: Timestamp = java.sql.Timestamp.valueOf("1900-01-01 00:00:00.0"),
                                 `version_id`: String = "1",
                                 `updated_on`: Timestamp = java.sql.Timestamp.valueOf("2021-11-16 00:18:52.146"),
                                 `created_on`: Timestamp = java.sql.Timestamp.valueOf("2021-11-16 00:18:52.146"),
                                 `profile`: Seq[String] = Seq("http://fhir.cqgc.ferlab.bio/StructureDefinition/cqgc-observation"),
                                 `patient_id`: String = "PA00001",
                                 `category_code`: Option[String] = None,
                                 `category_description`: Option[String] = None,
                                 `category_system`: Option[String] = None,
                                 `observation_code`: String = "CGH",
                                 `observation_code_description`: Option[String] = None,
                                 `observation_code_system`: String = "http://fhir.cqgc.ferlab.bio/CodeSystem/observation-code",
                                 `concept_values`: Option[Seq[CONCEPT_VALUE]] = Some(Seq(CONCEPT_VALUE())),
                                 `interpretation_code`: String = "affected",
                                 `interpretation_description`: String = "Abnormal",
                                 `interpretation_system`: String = "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation")

case class CONCEPT_VALUE(`concept_code`: Option[String] = None,
                         `concept_system`: Option[String] = None)
