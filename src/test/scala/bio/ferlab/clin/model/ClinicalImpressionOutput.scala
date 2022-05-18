/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2021-03-10T15:41:07.029
 */
package bio.ferlab.clin.model

import java.sql.{Date, Timestamp}


case class ClinicalImpressionOutput( //`clinical_impression_id`: String = "CI0005",
                                     `id`: String = "CI0005",
                                     `date`: Date = Date.valueOf("2019-06-20"),
                                     `age_at_event_in_days`: Long = 1315,
                                     `investigation`: List[INVESTIGATION] = List(INVESTIGATION()),
                                     `resource_type`: String = "ClinicalImpression",
                                     `status`: String = "in-progress",
                                     `ingestion_file_name`: String = "/raw/landing/fhir/ClinicalImpression/ClinicalImpression_0_19000101_000000.json",
                                     `ingested_on`: Timestamp = Timestamp.valueOf("2021-03-10 10:27:15.0"),
                                     `version_id`: String = "25",
                                     `updated_on`: Timestamp = Timestamp.valueOf("2020-12-17 13:14:41.572"),
                                     `created_on`: Timestamp = Timestamp.valueOf("2020-12-17 13:14:41.572"),
                                     `patient_id`: String = "PA0005",
                                     `practitioner_id`: String = "PR00105",
                                     observations: List[String] = List("OB0157","OB0206", "OB0255", "OB0005", "OB0054", "OB0070", "OB0108")
                                   )


case class ITEM(`reference`: String = "Observation/OB0157")

case class ClinicalImpressionCODE(`text`: String = "initial-examination")

case class INVESTIGATION(`code`: ClinicalImpressionCODE = ClinicalImpressionCODE(),
                         `item`: List[ITEM] = List(ITEM(), ITEM("Observation/OB0206"), ITEM("Observation/OB0255"), ITEM("Observation/OB0005"),
                           ITEM("Observation/OB0054"), ITEM("Observation/OB0070"), ITEM("Observation/OB0108"), ITEM("FamilyMemberHistory/FMH0005")))

