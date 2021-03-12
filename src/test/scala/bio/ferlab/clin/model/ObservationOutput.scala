/**
 * Generated by [[bio.ferlab.datalake.core.ClassGenerator]]
 * on 2021-03-11T08:00:09.914
 */
package bio.ferlab.clin.model

import java.sql.Timestamp


case class ObservationOutput(`categoryDescription`: String = "Laboratory",
                             `observationCode`: String = "PHENO",
                             `observationDescription`: String = "cgh",
                             `ageAtOnset`: String = "HP:0410280",
                             `hpoCategory`: String = "HP:0001626",
                             `id`: String = "OB0001",
                             `interpretationCode`: String = "NEG",
                             `interpretationDescriptionEN`: String = "Negative",
                             `interpretationDescriptionFR`: String = "Non observé",
                             `note`: List[String] = List("--"),
                             `resourceType`: String = "Observation",
                             `status`: String = "final",
                             `patientId`: String = "PA0001",
                             `conceptCode`: String = "HP:0001681",
                             `conceptDescription`: String = "Abnormal echocardiogram",
                             `ingestionFileName`: String = "/raw/landing/fhir/Observation/Observation_0_20210310_102715.json",
                             `ingestedOn`: Timestamp = Timestamp.valueOf("2021-03-10 10:27:15.0"),
                             `versionId`: String = "58",
                             `updatedOn`: Timestamp = Timestamp.valueOf("2020-12-17 13:14:26.581"),
                             `createdOn`: Timestamp = Timestamp.valueOf("2020-12-17 13:14:26.581"))



