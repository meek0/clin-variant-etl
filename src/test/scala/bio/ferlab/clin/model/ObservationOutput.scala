/**
 * Generated by [[bio.ferlab.datalake.core.ClassGenerator]]
 * on 2021-03-11T08:00:09.914
 */
package bio.ferlab.clin.model

import java.sql.Timestamp


case class ObservationOutput(//`observation_id`: String = "OB0001",
                             `id`: String = "OB0001",
                             `category_description`: String = "Laboratory",
                             `observation_code`: String = "PHENO",
                             `observation_description`: String = "cgh",
                             `age_at_onset`: String = "HP:0410280",
                             `hpo_category`: String = "HP:0001626",
                             `interpretation_code`: String = "NEG",
                             `interpretation_description_EN`: String = "Negative",
                             `interpretation_description_FR`: String = "Non observé",
                             `note`: List[String] = List("--"),
                             `resource_type`: String = "Observation",
                             `status`: String = "final",
                             `patient_id`: String = "PA0001",
                             `concept_code`: String = "HP:0001681",
                             `concept_description`: String = "Abnormal echocardiogram",
                             `ingestion_file_name`: String = "/raw/landing/fhir/Observation/Observation_0_19000101_102715.json",
                             `ingested_on`: Timestamp = Timestamp.valueOf("2021-03-10 10:27:15.0"),
                             `version_id`: String = "58",
                             `updated_on`: Timestamp = Timestamp.valueOf("2020-12-17 13:14:26.581"),
                             `created_on`: Timestamp = Timestamp.valueOf("2020-12-17 13:14:26.581"))



