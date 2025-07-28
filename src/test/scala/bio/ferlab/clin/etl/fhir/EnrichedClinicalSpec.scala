package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.model.normalized.fhir._
import bio.ferlab.clin.model.enriched.{ClinicalSign, EnrichedClinical => EnrichedClinicalOutput}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.{ClassGenerator, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

import java.sql.Date

class EnrichedClinicalSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val normalized_clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val normalized_code_system: DatasetConf = conf.getDataset("normalized_code_system")
  val normalized_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  val normalized_family: DatasetConf = conf.getDataset("normalized_family")
  val normalized_observation: DatasetConf = conf.getDataset("normalized_observation")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_person: DatasetConf = conf.getDataset("normalized_person")
  val normalized_service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val normalized_specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val normalized_task: DatasetConf = conf.getDataset("normalized_task")

  /**
   * USE CASES
   *
   * 1. Happy path: Trio
   *    - Proband, mother, father
   *    - Germline analysis
   *
   * 2. Happy path: Trio+
   *    - Proband, brother, mother, father
   *    - Tumor only analysis
   *
   * 3. Incomplete trio
   *    - Proband, mother in a batch
   *    - Father received in a later batch
   *    - Germline analysis
   *    - The same prescription/analysis is re-used (not supported by the app yet)
   *
   * 4. Same family, two prescriptions
   * 4.1 First prescription: Duo
   *      - Proband (daughter), mother
   *      - Germline analysis
   *        4.2 Second prescription: Trio
   *      - Proband (other daughter), sister (from 3.1), mother (from 3.1)
   *      - Germline analysis
   *      - A new prescription/analysis is created
   *      - The mother is re-sequenced
   *      - The sister from 3.1 is not really re-sequenced but we receive a new sequencing request
   *
   * 5. Solo
   *    - Proband
   *    - Tumor normal analysis
   *    - 3 tasks are created in different batches : 1. tumor_only, 2. germline, 3. tumor_normal
   */

  val patientDf: DataFrame = Seq(
    // 1. Trio
    NormalizedPatient(`id` = "PA0001", `gender` = "male", `practitioner_role_id` = "PPR00101"), // proband
    NormalizedPatient(`id` = "PA0002", `gender` = "male", `practitioner_role_id` = "PPR00101"), // father
    NormalizedPatient(`id` = "PA0003", `gender` = "female", `practitioner_role_id` = "PPR00101"), // mother

    // 2. Trio+
    NormalizedPatient(`id` = "PA0004", `gender` = "male", `practitioner_role_id` = "PPR00101"), // proband
    NormalizedPatient(`id` = "PA0005", `gender` = "male", `practitioner_role_id` = "PPR00101"), // brother
    NormalizedPatient(`id` = "PA0006", `gender` = "male", `practitioner_role_id` = "PPR00101"), // father
    NormalizedPatient(`id` = "PA0007", `gender` = "female", `practitioner_role_id` = "PPR00101"), // mother

    // 3. Incomplete trio
    NormalizedPatient(`id` = "PA0011", `gender` = "male", `practitioner_role_id` = "PPR00102"), // proband
    NormalizedPatient(`id` = "PA0022", `gender` = "female", `practitioner_role_id` = "PPR00102"), // mother
    NormalizedPatient(`id` = "PA0033", `gender` = "male", `practitioner_role_id` = "PPR00102"), // father we receive later

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedPatient(`id` = "PA0111", `gender` = "female", `practitioner_role_id` = "PPR00102"), // proband
    NormalizedPatient(`id` = "PA0222", `gender` = "female", `practitioner_role_id` = "PPR00102"), // mother
    // 4.2 Second prescription: Trio
    NormalizedPatient(`id` = "PA0333", `gender` = "female", `practitioner_role_id` = "PPR00102"), // proband

    // 5. Solo
    NormalizedPatient(`id` = "PA1111", `gender` = "female", `practitioner_role_id` = "PPR00103"), // proband

    // 6. API Solo WGS
    NormalizedPatient(`id` = "API-PA-0001", `gender` = "female", `practitioner_role_id` = "PPR00103"), // proband
  ).toDF()

  val personDf: DataFrame = Seq(
    NormalizedPerson(`id` = "P0001", `patient_ids` = Seq("PA0001", "PA0002"), `first_name` = "John"),
    NormalizedPerson(`id` = "P0002", `patient_ids` = Seq("PA0003"))
  ).toDF()

  val serviceRequestDf: DataFrame = Seq(
    // 1. Trio
    NormalizedServiceRequest(service_request_type = "analysis", `patient_id` = "PA0001", `id` = "SRA0001", `clinical_impressions` = Some(Seq("CI0001", "CI0002", "CI0003"))),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0001", `id` = "SRS0001", analysis_service_request_id = Some("SRA0001")),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0002", `id` = "SRS0002", analysis_service_request_id = Some("SRA0001")),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0003", `id` = "SRS0003", analysis_service_request_id = Some("SRA0001")),

    // 2. Trio+
    NormalizedServiceRequest(service_request_type = "analysis", `patient_id` = "PA0004", `id` = "SRA0004", `clinical_impressions` = Some(Seq("CI0004", "CI0005", "CI0006", "CI0007"))),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0004", `id` = "SRS0004", analysis_service_request_id = Some("SRA0004")),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0005", `id` = "SRS0005", analysis_service_request_id = Some("SRA0004")),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0006", `id` = "SRS0006", analysis_service_request_id = Some("SRA0004")),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0007", `id` = "SRS0007", analysis_service_request_id = Some("SRA0004")),

    // 3. Incomplete trio
    // Analysis service request was updated once we received the father
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0011", `id` = "SRS0011", analysis_service_request_id = Some("SRA0011")),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0022", `id` = "SRS0022", analysis_service_request_id = Some("SRA0011")),
    NormalizedServiceRequest(service_request_type = "analysis", `patient_id` = "PA0011", `id` = "SRA0011", `clinical_impressions` = Some(Seq("CI0011", "CI0022", "CI0033"))), // Updated with father CI
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0033", `id` = "SRS0033", analysis_service_request_id = Some("SRA0011")), // Father we receive later

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedServiceRequest(service_request_type = "analysis", `patient_id` = "PA0111", `id` = "SRA0111", `clinical_impressions` = Some(Seq("CI0111", "CI0222"))),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0111", `id` = "SRS0111", analysis_service_request_id = Some("SRA0111")),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0222", `id` = "SRS0222", analysis_service_request_id = Some("SRA0111")),
    // 4.2 Second prescription: Trio
    NormalizedServiceRequest(service_request_type = "analysis", `patient_id` = "PA0333", `id` = "SRA0333", `clinical_impressions` = Some(Seq("CI0333", "CI0444", "CI0555"))),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0333", `id` = "SRS0333", analysis_service_request_id = Some("SRA0333")), // proband
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0111", `id` = "SRS0444", analysis_service_request_id = Some("SRA0333")), // sister
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA0222", `id` = "SRS0555", analysis_service_request_id = Some("SRA0333")), // mother

    // 5. Solo
    // Tumor only analysis
    NormalizedServiceRequest(service_request_type = "analysis", `patient_id` = "PA1111", `id` = "SRA1111", `clinical_impressions` = Some(Seq("CI1111"))),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA1111", `id` = "SRS1111", analysis_service_request_id = Some("SRA1111")),
    // Germline analysis
    NormalizedServiceRequest(service_request_type = "analysis", `patient_id` = "PA1111", `id` = "SRA2222", `clinical_impressions` = Some(Seq("CI2222"))),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "PA1111", `id` = "SRS2222", analysis_service_request_id = Some("SRA2222")),

    // 6. API Solo WGS
    // Germline analysis
    NormalizedServiceRequest(service_request_type = "analysis", `patient_id` = "API-PA-0001", `id` = "API-SRA-0001", `clinical_impressions` = Some(Seq("API-CI-0001"))),
    NormalizedServiceRequest(service_request_type = "sequencing", `patient_id` = "API-PA-0001", `id` = "API-SRS-0001", analysis_service_request_id = Some("API-SRA-0001")),
  ).toDF()

  val clinicalImpressionsDf: DataFrame = Seq(
    // 1. Trio
    NormalizedClinicalImpression(id = "CI0001", `patient_id` = "PA0001", observations = List("OB0001", "OB000X", "OB0001-A")),
    NormalizedClinicalImpression(id = "CI0002", `patient_id` = "PA0002", observations = List("OB0002")),
    NormalizedClinicalImpression(id = "CI0003", `patient_id` = "PA0003", observations = List("OB0003", "OB0003-A")),

    // 2. Trio+
    NormalizedClinicalImpression(id = "CI0004", `patient_id` = "PA0004", observations = List("OB0004", "OB0004-A", "OB0004-B")),
    NormalizedClinicalImpression(id = "CI0005", `patient_id` = "PA0005", observations = List("OB0005", "OB0005-A", "OB0005-B")),
    NormalizedClinicalImpression(id = "CI0006", `patient_id` = "PA0006", observations = List("OB0006")),
    NormalizedClinicalImpression(id = "CI0007", `patient_id` = "PA0007", observations = List("OB0007")),

    // 3. Incomplete trio
    NormalizedClinicalImpression(id = "CI0011", `patient_id` = "PA0011", observations = List("OB0011", "OB0011-A", "OB0011-B")),
    NormalizedClinicalImpression(id = "CI0022", `patient_id` = "PA0022", observations = List("OB0022", "OB0022-A", "OB0022-B", "OB0022-C")),
    NormalizedClinicalImpression(id = "CI0033", `patient_id` = "PA0033", observations = List("OB0033", "OB0033-A", "OB0033-B", "OB0033-C")),

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedClinicalImpression(id = "CI0111", `patient_id` = "PA0111", observations = List("OB0111", "OB0XXX", "OB0111-A")),
    NormalizedClinicalImpression(id = "CI0222", `patient_id` = "PA0222", observations = List("OB0222", "OB0YYY", "OB0ZZZ")),
    // 4.2 Second prescription: Trio
    NormalizedClinicalImpression(id = "CI0333", `patient_id` = "PA0333", observations = List("OB0333", "OB0333-A")),
    NormalizedClinicalImpression(id = "CI0444", `patient_id` = "PA0111", observations = List("OB0444")),
    NormalizedClinicalImpression(id = "CI0555", `patient_id` = "PA0222", observations = List("OB0555", "OB0555-A")),

    // 5. Solo
    // Clinical impression for tumor only analysis
    NormalizedClinicalImpression(id = "CI1111", `patient_id` = "PA1111", observations = List("OB1111", "OB1111-A", "OB1111-B", "OB1111-C")),
    // Clinical impression for germline analysis
    NormalizedClinicalImpression(id = "CI2222", `patient_id` = "PA1111", observations = List("OB2222", "OB2222-A", "OB2222-B", "OB2222-C")),

    // 6. API Solo WGS
    // Germline analysis
    NormalizedClinicalImpression(id = "API-CI-0001", `patient_id` = "API-PA-0001", observations = List("API-OBS-0001")),
  ).toDF()

  val observationsDf: DataFrame = Seq(
    // All affected patients have at least one clinical signs
    // 1. Trio, All affected patients have the same clinical signs
    NormalizedObservation(id = "OB0001"  , patient_id = "PA0001", `observation_code` = "DSTA" , `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB000X"  , patient_id = "PA0001", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0001-A", patient_id = "PA0001", `observation_code` = "PHEN" , `interpretation_code` = "affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),
    NormalizedObservation(id = "OB0002"  , patient_id = "PA0002", `observation_code` = "DSTA" , `interpretation_code` = "not_affected"),
    NormalizedObservation(id = "OB0003"  , patient_id = "PA0003", `observation_code` = "DSTA" , `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0003-A", patient_id = "PA0003", `observation_code` = "PHEN" , `interpretation_code` = "affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),

    // 2. Trio+, Proband has one extra clinical sign, brother is unaffected
    NormalizedObservation(id = "OB0004"  , patient_id = "PA0004", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0004-A", patient_id = "PA0004", `observation_code` = "PHEN", `interpretation_code` = "affected"    , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),
    NormalizedObservation(id = "OB0004-B", patient_id = "PA0004", `observation_code` = "PHEN", `interpretation_code` = "affected"    , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000002"))))),
    NormalizedObservation(id = "OB0005"  , patient_id = "PA0005", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0005-A", patient_id = "PA0005", `observation_code` = "PHEN", `interpretation_code` = "affected"    , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),
    NormalizedObservation(id = "OB0005-B", patient_id = "PA0004", `observation_code` = "PHEN", `interpretation_code` = "not_affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000002"))))),
    NormalizedObservation(id = "OB0006"  , patient_id = "PA0006", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    NormalizedObservation(id = "OB0007"  , patient_id = "PA0007", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),

    // 3. Incomplete trio, Everyone has at least one common clinical sign with some unknown
    NormalizedObservation(id = "OB0011"  , patient_id = "PA0011", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0011-A", patient_id = "PA0011", `observation_code` = "PHEN", `interpretation_code` = "affected"    , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),
    NormalizedObservation(id = "OB0011-B", patient_id = "PA0011", `observation_code` = "PHEN", `interpretation_code` = "not_affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000002"))))),
    NormalizedObservation(id = "OB0022"  , patient_id = "PA0022", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0022-A", patient_id = "PA0022", `observation_code` = "PHEN", `interpretation_code` = "affected"    , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),
    NormalizedObservation(id = "OB0022-B", patient_id = "PA0022", `observation_code` = "PHEN", `interpretation_code` = "not_affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000002"))))),
    NormalizedObservation(id = "OB0022-C", patient_id = "PA0022", `observation_code` = "PHEN", `interpretation_code` = "unknown"     , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000003"))))),
    NormalizedObservation(id = "OB0033"  , patient_id = "PA0033", `observation_code` = "DSTA", `interpretation_code` = "affected"), // Received later
    NormalizedObservation(id = "OB0033-A", patient_id = "PA0033", `observation_code` = "PHEN", `interpretation_code` = "affected"    , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),
    NormalizedObservation(id = "OB0033-B", patient_id = "PA0033", `observation_code` = "PHEN", `interpretation_code` = "unknown"     , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000002"))))),
    NormalizedObservation(id = "OB0033-C", patient_id = "PA0033", `observation_code` = "PHEN", `interpretation_code` = "affected"    , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000003"))))),

    // 4. Same family, two prescriptions, Affected patients have the same clinical signs
    // 4.1 First prescription: Duo
    NormalizedObservation(id = "OB0111"  , patient_id = "PA0111", `observation_code` = "DSTA" , `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0111-A", patient_id = "PA0111", `observation_code` = "PHEN" , `interpretation_code` = "affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),
    NormalizedObservation(id = "OB0222"  , patient_id = "PA0222", `observation_code` = "DSTA" , `interpretation_code` = "not_affected"),
    NormalizedObservation(id = "OB0XXX"  , patient_id = "PA0111", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0YYY"  , patient_id = "PA0222", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0ZZZ"  , patient_id = "PA0222", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    // 4.2 Second prescription: Trio, Testing for a different clinical sign
    NormalizedObservation(id = "OB0333"  , patient_id = "PA0333", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0333-A", patient_id = "PA0333", `observation_code` = "PHEN", `interpretation_code` = "affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000002"))))),
    NormalizedObservation(id = "OB0444"  , patient_id = "PA0111", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    NormalizedObservation(id = "OB0555"  , patient_id = "PA0222", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0555-A", patient_id = "PA0222", `observation_code` = "PHEN", `interpretation_code` = "affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000002"))))),

    // 5. Solo, Combination of affected, not affected and unknown clinical signs
    // Observation for tumor only analysis
    NormalizedObservation(id = "OB1111"  , patient_id = "PA1111", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB1111-A", patient_id = "PA1111", `observation_code` = "PHEN", `interpretation_code` = "affected"    , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),
    NormalizedObservation(id = "OB1111-B", patient_id = "PA1111", `observation_code` = "PHEN", `interpretation_code` = "not_affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000002"))))),
    NormalizedObservation(id = "OB1111-C", patient_id = "PA1111", `observation_code` = "PHEN", `interpretation_code` = "unknown"     , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000003"))))),
    // Observation for germline analysis
    NormalizedObservation(id = "OB2222"  , patient_id = "PA1111", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB2222-A", patient_id = "PA1111", `observation_code` = "PHEN", `interpretation_code` = "affected"    , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000001"))))),
    NormalizedObservation(id = "OB2222-B", patient_id = "PA1111", `observation_code` = "PHEN", `interpretation_code` = "not_affected", `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000002"))))),
    NormalizedObservation(id = "OB2222-C", patient_id = "PA1111", `observation_code` = "PHEN", `interpretation_code` = "unknown"     , `concept_values` = Some(Seq(CONCEPT_VALUE(concept_code = Some("HP:0000003"))))),

    // 6. API Solo WGS
    // Germline analysis
    NormalizedObservation(id = "API-OBS-0001"  , patient_id = "API-PA-0001", `observation_code` = "DSTA", `interpretation_code` = "affected"),
  ).toDF()

  val codeSystemDf: DataFrame = Seq(
    NormalizedCodeSystem(`id` = "hp", `concept_code` = "HP:0000001", `concept_description` = "Term 1"),
    NormalizedCodeSystem(`id` = "hp", `concept_code` = "HP:0000002", `concept_description` = "Term 2"),
    NormalizedCodeSystem(`id` = "hp", `concept_code` = "HP:0000003", `concept_description` = "Term 3"),
  ).toDF()

  val familyDf: DataFrame = Seq(
    // 1. Trio
    NormalizedFamily(analysis_service_request_id = "SRA0001", patient_id = "PA0001", family = Some(FAMILY(mother = Some("PA0003"), father = Some("PA0002"))), family_id = Some("FM00001")), // proband
    NormalizedFamily(analysis_service_request_id = "SRA0001", patient_id = "PA0002", family = None, family_id = Some("FM00001")), // father
    NormalizedFamily(analysis_service_request_id = "SRA0001", patient_id = "PA0003", family = None, family_id = Some("FM00001")), // mother

    // 2. Trio+
    NormalizedFamily(analysis_service_request_id = "SRA0004", patient_id = "PA0004", family = Some(FAMILY(mother = Some("PA0007"), father = Some("PA0006"))), family_id = Some("FM00004")), // proband
    NormalizedFamily(analysis_service_request_id = "SRA0004", patient_id = "PA0005", family = Some(FAMILY(mother = Some("PA0007"), father = Some("PA0006"))), family_id = Some("FM00004")), // brother
    NormalizedFamily(analysis_service_request_id = "SRA0004", patient_id = "PA0006", family = None, family_id = Some("FM00004")), // father
    NormalizedFamily(analysis_service_request_id = "SRA0004", patient_id = "PA0007", family = None, family_id = Some("FM00004")), // mother

    // 3. Incomplete trio
    // Family resource was updated once we received the father
    NormalizedFamily(analysis_service_request_id = "SRA0011", patient_id = "PA0011", family = Some(FAMILY(mother = Some("PA0022"), father = Some("PA0033"))), family_id = Some("FM00011")), // proband
    NormalizedFamily(analysis_service_request_id = "SRA0011", patient_id = "PA0022", family = None, family_id = Some("FM00011")), // mother
    NormalizedFamily(analysis_service_request_id = "SRA0011", patient_id = "PA0033", family = None, family_id = Some("FM00011")), // father we receive later

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedFamily(analysis_service_request_id = "SRA0111", patient_id = "PA0111", family = Some(FAMILY(mother = Some("PA0222"), father = None)), family_id = Some("FM00111")), // proband
    NormalizedFamily(analysis_service_request_id = "SRA0111", patient_id = "PA0222", family = None, family_id = Some("FM00111")), // mother
    // 4.2 Second prescription: Trio
    NormalizedFamily(analysis_service_request_id = "SRA0333", patient_id = "PA0333", family = Some(FAMILY(mother = Some("PA0222"), father = None)), family_id = Some("FM00222")), // proband
    NormalizedFamily(analysis_service_request_id = "SRA0333", patient_id = "PA0111", family = Some(FAMILY(mother = Some("PA0222"), father = None)), family_id = Some("FM00222")), // sister from previous analysis
    NormalizedFamily(analysis_service_request_id = "SRA0333", patient_id = "PA0222", family = None, family_id = Some("FM00222")), // mother

    // 5. Solo
    // Tumor only analysis
    NormalizedFamily(analysis_service_request_id = "SRA1111", patient_id = "PA1111", family = None, family_id = None),
    // Germline analysis
    NormalizedFamily(analysis_service_request_id = "SRS2222", patient_id = "PA1111", family = None, family_id = None)
  ).toDF()

  val taskDf: DataFrame = Seq(
    // To help readability, id is composed of <patient_id>-<service_request_id>-<analysis_code>
    // BATCH 1
    // 1. Trio, germline
    NormalizedTask(id = "1-1-G", batch_id = "BAT1", `patient_id` = "PA0001", `service_request_id` = "SRS0001", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "1"),
      documents = List(DOCUMENTS(id = "1-1-G-COVGENE", document_type = "COVGENE"), DOCUMENTS(id = "1-1-G-EXOMISER", document_type = "EXOMISER"), DOCUMENTS(id = "1-1-G-GCNV", document_type = "GCNV"))),
    NormalizedTask(id = "2-2-G", batch_id = "BAT1", `patient_id` = "PA0002", `service_request_id` = "SRS0002", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "2"),
      documents = List(DOCUMENTS(id = "2-2-G-COVGENE", document_type = "COVGENE"), DOCUMENTS(id = "2-2-G-EXOMISER", document_type = "EXOMISER"), DOCUMENTS(id = "2-2-G-GCNV", document_type = "GCNV"))),
    NormalizedTask(id = "3-3-G", batch_id = "BAT1", `patient_id` = "PA0003", `service_request_id` = "SRS0003", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "3"),
      documents = List(DOCUMENTS(id = "3-3-G-COVGENE", document_type = "COVGENE"), DOCUMENTS(id = "3-3-G-EXOMISER", document_type = "EXOMISER"), DOCUMENTS(id = "3-3-G-GCNV", document_type = "GCNV"))),

    // 2. Trio+, tumor only
    NormalizedTask(id = "4-4-TO", batch_id = "BAT1", `patient_id` = "PA0004", `service_request_id` = "SRS0004", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "4"),
      documents = List(DOCUMENTS(id = "4-4-TO-EXOMISER", document_type = "EXOMISER"))),
    NormalizedTask(id = "5-5-TO", batch_id = "BAT1", `patient_id` = "PA0005", `service_request_id` = "SRS0005", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "5"),
      documents = List(DOCUMENTS(id = "5-5-TO-EXOMISER", document_type = "EXOMISER"))),
    NormalizedTask(id = "6-6-TO", batch_id = "BAT1", `patient_id` = "PA0006", `service_request_id` = "SRS0006", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "6"),
      documents = List(DOCUMENTS(id = "6-6-TO-EXOMISER", document_type = "EXOMISER"))),
    NormalizedTask(id = "7-7-TO", batch_id = "BAT1", `patient_id` = "PA0007", `service_request_id` = "SRS0007", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "7"),
      documents = List(DOCUMENTS(id = "7-7-TO-EXOMISER", document_type = "EXOMISER"))),

    // BATCH 2
    // 3. Incomplete trio
    NormalizedTask(id = "11-11-G", batch_id = "BAT2", `patient_id` = "PA0011", `service_request_id` = "SRS0011", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "11"), // proband
      documents = List(DOCUMENTS(id = "11-11-G-COVGENE", document_type = "COVGENE"))),
    NormalizedTask(id = "22-22-G", batch_id = "BAT2", `patient_id` = "PA0022", `service_request_id` = "SRS0022", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "22"), // mother
      documents = List(DOCUMENTS(id = "22-22-G-COVGENE", document_type = "COVGENE"))),

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedTask(id = "111-111-G", batch_id = "BAT2", `patient_id` = "PA0111", `service_request_id` = "SRS0111", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "111"),
      documents = List(DOCUMENTS(id = "111-111-G-COVGENE", document_type = "COVGENE"))),
    NormalizedTask(id = "222-222-G", batch_id = "BAT2", `patient_id` = "PA0222", `service_request_id` = "SRS0222", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "222"),
      documents = List(DOCUMENTS(id = "222-222-G-COVGENE", document_type = "COVGENE"))),

    // BATCH 3
    // 3. Incomplete trio, father's task
    NormalizedTask(id = "33-33-G", batch_id = "BAT3", `patient_id` = "PA0033", `service_request_id` = "SRS0033", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "33"),
      documents = List(DOCUMENTS(id = "33-33-G-COVGENE", document_type = "COVGENE"))), // father

    // 4. Same family, two prescriptions
    // 4.2 Second prescription: Trio
    NormalizedTask(id = "333-333-G", batch_id = "BAT3", `patient_id` = "PA0333", `service_request_id` = "SRS0333", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "333"), // proband
      documents = List(DOCUMENTS(id = "333-333-G-COVGENE", document_type = "COVGENE"), DOCUMENTS(id = "333-333-G-EXOMISER", document_type = "EXOMISER"), DOCUMENTS(id = "333-333-G-EXOMISER_CNV", document_type = "EXOMISER_CNV"), DOCUMENTS(id = "333-333-G-SNV", document_type = "SNV"))),
    NormalizedTask(id = "111-444-G", batch_id = "BAT3", `patient_id` = "PA0111", `service_request_id` = "SRS0444", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "111"), // sister (same aliquot since she's not re-sequenced)
      documents = List(DOCUMENTS(id = "111-444-G-SNV", document_type = "SNV"))),
    NormalizedTask(id = "222-555-G", batch_id = "BAT3", `patient_id` = "PA0222", `service_request_id` = "SRS0555", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "555"), // mother
      documents = List(DOCUMENTS(id = "222-555-G-COVGENE", document_type = "COVGENE"), DOCUMENTS(id = "222-555-G-SNV", document_type = "SNV"))),

    // 5. Solo, Tumor only analysis
    NormalizedTask(id = "1111-1111-TO", batch_id = "BAT3", `patient_id` = "PA1111", `service_request_id` = "SRS1111", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "1111"),
      documents = List(DOCUMENTS(id = "1111-1111-TO-SNV", document_type = "SNV"))),

    // BATCH 4
    // 5. Solo, Germline analysis
    NormalizedTask(id = "1111-2222-G", batch_id = "BAT4", `patient_id` = "PA1111", `service_request_id` = "SRS2222", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "2222"),
      documents = List(DOCUMENTS(id = "1111-2222-G-SNV", document_type = "SNV"))),

    // BATCH 5
    // 5. Solo, tumor normal (same servie_request_id and aliquot_id as tumor only)
    NormalizedTask(id = "1111-1111-TN", batch_id = "BAT5", `patient_id` = "PA1111", `service_request_id` = "SRS1111", `analysis_code` = "TNEBA", `experiment` = EXPERIMENT(`aliquot_id` = "1111"),
      documents = List(DOCUMENTS(id = "1111-1111-TN-SNV", document_type = "SNV"))),

    // 6. API Solo WGS
    // Germline analysis
    NormalizedTask(id = "API-0001-TN", batch_id = "API_BAT_01", `patient_id` = "API-PA-0001", `service_request_id` = "API-SRS-0001", `analysis_code` = "GEBA", `experiment` = EXPERIMENT(`aliquot_id` = "API-SP-0001", `sequencing_strategy` = "WGS"),
      documents = List(DOCUMENTS(id = "API-DOC-01-SNV", document_type = "SNV"))),
  ).toDF()

  val specimenDf: DataFrame = Seq(
    // 1. Trio
    NormalizedSpecimen(`id` = "1-1", `patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = Some("SA_0001"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "1-2", `patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = None, `specimen_id` = Some("SP_0001")),
    NormalizedSpecimen(`id` = "2-1", `patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = Some("SA_0002"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "2-2", `patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = None, `specimen_id` = Some("SP_0002")),
    NormalizedSpecimen(`id` = "3-1", `patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = Some("SA_0003"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "3-2", `patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = None, `specimen_id` = Some("SP_0003")),

    // 2. Trio+
    NormalizedSpecimen(`id` = "4-1", `patient_id` = "PA0004", `service_request_id` = "SRS0004", `sample_id` = Some("SA_0004"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "4-2", `patient_id` = "PA0004", `service_request_id` = "SRS0004", `sample_id` = None, `specimen_id` = Some("SP_0004")),
    NormalizedSpecimen(`id` = "5-1", `patient_id` = "PA0005", `service_request_id` = "SRS0005", `sample_id` = Some("SA_0005"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "5-2", `patient_id` = "PA0005", `service_request_id` = "SRS0005", `sample_id` = None, `specimen_id` = Some("SP_0005")),
    NormalizedSpecimen(`id` = "6-1", `patient_id` = "PA0006", `service_request_id` = "SRS0006", `sample_id` = Some("SA_0006"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "6-2", `patient_id` = "PA0006", `service_request_id` = "SRS0006", `sample_id` = None, `specimen_id` = Some("SP_0006")),
    NormalizedSpecimen(`id` = "7-1", `patient_id` = "PA0007", `service_request_id` = "SRS0007", `sample_id` = Some("SA_0007"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "7-2", `patient_id` = "PA0007", `service_request_id` = "SRS0007", `sample_id` = None, `specimen_id` = Some("SP_0007")),

    // 3. Incomplete trio
    NormalizedSpecimen(`id` = "11-1", `patient_id` = "PA0011", `service_request_id` = "SRS0011", `sample_id` = Some("SA_0011"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "11-2", `patient_id` = "PA0011", `service_request_id` = "SRS0011", `sample_id` = None, `specimen_id` = Some("SP_0011")),
    NormalizedSpecimen(`id` = "22-1", `patient_id` = "PA0022", `service_request_id` = "SRS0022", `sample_id` = Some("SA_0022"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "22-2", `patient_id` = "PA0022", `service_request_id` = "SRS0022", `sample_id` = None, `specimen_id` = Some("SP_0022")),
    NormalizedSpecimen(`id` = "33-1", `patient_id` = "PA0033", `service_request_id` = "SRS0033", `sample_id` = Some("SA_0033"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "33-2", `patient_id` = "PA0033", `service_request_id` = "SRS0033", `sample_id` = None, `specimen_id` = Some("SP_0033")),

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedSpecimen(`id` = "111-1", `patient_id` = "PA0111", `service_request_id` = "SRS0111", `sample_id` = Some("SA_0111"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "111-2", `patient_id` = "PA0111", `service_request_id` = "SRS0111", `sample_id` = None, `specimen_id` = Some("SP_0111")),
    NormalizedSpecimen(`id` = "222-1", `patient_id` = "PA0222", `service_request_id` = "SRS0222", `sample_id` = Some("SA_0222"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "222-2", `patient_id` = "PA0222", `service_request_id` = "SRS0222", `sample_id` = None, `specimen_id` = Some("SP_0222")),

    // 4.2 Second prescription: Trio
    NormalizedSpecimen(`id` = "333-1", `patient_id` = "PA0333", `service_request_id` = "SRS0333", `sample_id` = Some("SA_0333"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "333-2", `patient_id` = "PA0333", `service_request_id` = "SRS0333", `sample_id` = None, `specimen_id` = Some("SP_0333")),
    NormalizedSpecimen(`id` = "444-1", `patient_id` = "PA0111", `service_request_id` = "SRS0444", `sample_id` = Some("SA_0111"), `specimen_id` = None), // Same sample_id and specimen_id as past sequencing
    NormalizedSpecimen(`id` = "444-2", `patient_id` = "PA0111", `service_request_id` = "SRS0444", `sample_id` = None, `specimen_id` = Some("SP_0111")),
    NormalizedSpecimen(`id` = "555-1", `patient_id` = "PA0222", `service_request_id` = "SRS0555", `sample_id` = Some("SA_0555"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "555-2", `patient_id` = "PA0222", `service_request_id` = "SRS0555", `sample_id` = None, `specimen_id` = Some("SP_0555")),

    // 5. Solo
    // Tumor only analysis
    NormalizedSpecimen(`id` = "1111-1", `patient_id` = "PA1111", `service_request_id` = "SRS1111", `sample_id` = Some("SA_1111"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "1111-2", `patient_id` = "PA1111", `service_request_id` = "SRS1111", `sample_id` = None, `specimen_id` = Some("SP_1111")),
    // Germline analysis
    NormalizedSpecimen(`id` = "2222-1", `patient_id` = "PA1111", `service_request_id` = "SRS2222", `sample_id` = Some("SA_2222"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "2222-2", `patient_id` = "PA1111", `service_request_id` = "SRS2222", `sample_id` = None, `specimen_id` = Some("SP_2222")),

    // 6. API Solo WGS
    // Germline analysis
    NormalizedSpecimen(`id` = "2222-1", `patient_id` = "API-PA-0001", `service_request_id` = "API-SRS-0001", `sample_id` = Some("API-SA-0001"), `specimen_id` = None),
    NormalizedSpecimen(`id` = "2222-2", `patient_id` = "API-PA-0001", `service_request_id` = "API-SRS-0001", `sample_id` = None, `specimen_id` = Some("API-SP-0001")),
  ).toDF

  val documentDf: DataFrame = Seq(
    // To help readability, id is composed of <patient_id>-<service_request_id>-<analysis_code>-<type>
    // 1. Trio: Everyone with Covgene, Exomiser and CNV
    NormalizedDocumentReference(id = "1-1-G-COVGENE", patient_id = "PA0001", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://1.csv"))),
    NormalizedDocumentReference(id = "1-1-G-EXOMISER", patient_id = "PA0001", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://1.tsv"), Content(format = "JSON", s3_url = "s3a://1.json"))),
    NormalizedDocumentReference(id = "1-1-G-GCNV", patient_id = "PA0001", `type` = "GCNV", contents = List(Content(format = "VCF", s3_url = "s3a://1-1.vcf.gz"), Content(format = "VCF", s3_url = "s3a://1-2.vcf.gz"))),
    NormalizedDocumentReference(id = "2-2-G-COVGENE", patient_id = "PA0002", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://2.csv"))),
    NormalizedDocumentReference(id = "2-2-G-EXOMISER", patient_id = "PA0002", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://2.tsv"), Content(format = "JSON", s3_url = "s3a://2.json"))),
    NormalizedDocumentReference(id = "2-2-G-GCNV", patient_id = "PA0002", `type` = "GCNV", contents = List(Content(format = "VCF", s3_url = "s3a://2-1.vcf.gz"), Content(format = "VCF", s3_url = "s3a://2-2.vcf.gz"))),
    NormalizedDocumentReference(id = "3-3-G-COVGENE", patient_id = "PA0003", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://3.csv"))),
    NormalizedDocumentReference(id = "3-3-G-EXOMISER", patient_id = "PA0003", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://3.tsv"), Content(format = "JSON", s3_url = "s3a://3.json"))),
    NormalizedDocumentReference(id = "3-3-G-GCNV", patient_id = "PA0003", `type` = "GCNV", contents = List(Content(format = "VCF", s3_url = "s3a://3-1.vcf.gz"), Content(format = "VCF", s3_url = "s3a://3-2.vcf.gz"))),

    // 2. Trio+: Everyone with Exomiser
    NormalizedDocumentReference(id = "4-4-TO-EXOMISER", patient_id = "PA0004", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://4.tsv"), Content(format = "JSON", s3_url = "s3a://4.json"))),
    NormalizedDocumentReference(id = "5-5-TO-EXOMISER", patient_id = "PA0005", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://5.tsv"), Content(format = "JSON", s3_url = "s3a://5.json"))),
    NormalizedDocumentReference(id = "6-6-TO-EXOMISER", patient_id = "PA0006", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://6.tsv"), Content(format = "JSON", s3_url = "s3a://6.json"))),
    NormalizedDocumentReference(id = "7-7-TO-EXOMISER", patient_id = "PA0007", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://7.tsv"), Content(format = "JSON", s3_url = "s3a://7.json"))),

    // 3. Incomplete trio: Multiple Covgene files per specimen
    NormalizedDocumentReference(id = "11-11-G-COVGENE", patient_id = "PA0011", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://11-1.csv"), Content(format = "CSV", s3_url = "s3a://11-2.csv"), Content(format = "CSV", s3_url = "s3a://11-3.csv"))),
    NormalizedDocumentReference(id = "22-22-G-COVGENE", patient_id = "PA0022", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://22-1.csv"), Content(format = "CSV", s3_url = "s3a://22-2.csv"))),
    NormalizedDocumentReference(id = "33-33-G-COVGENE", patient_id = "PA0033", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://33.csv"))),

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo -- Only SNV TBI files that should not appear in table
    NormalizedDocumentReference(id = "111-111-G-SNV", patient_id = "PA0111", `type` = "SNV", contents = List(Content(format = "TBI", s3_url = "s3a://111-1.vcf.gz.tbi"))),
    NormalizedDocumentReference(id = "222-222-G-SNV", patient_id = "PA0222", `type` = "SNV", contents = List(Content(format = "TBI", s3_url = "s3a://222.vcf.gz.tbi"))),
    // 4.2 Second prescription: Trio -- Mix of files
    NormalizedDocumentReference(id = "333-333-G-COVGENE", patient_id = "PA0333", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://333.csv"))),
    NormalizedDocumentReference(id = "333-333-G-EXOMISER", patient_id = "PA0333", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://333.tsv"), Content(format = "JSON", s3_url = "s3a://333.json"))),
    NormalizedDocumentReference(id = "333-333-G-EXOMISER_CNV", patient_id = "PA0333", `type` = "EXOMISER_CNV", contents = List(Content(format = "TSV", s3_url = "s3a://333_cnv.tsv"), Content(format = "JSON", s3_url = "s3a://333_cnv.json"))),
    NormalizedDocumentReference(id = "333-333-G-SNV", patient_id = "PA0333", `type` = "SNV", contents = List(Content(format = "TBI", s3_url = "s3a://333.vcf.gz.tbi"))),
    NormalizedDocumentReference(id = "111-444-G-SNV", patient_id = "PA0111", `type` = "SNV", contents = List(Content(format = "TBI", s3_url = "s3a://111-2.vcf.gz.tbi"))), // Same file as before but in different path
    NormalizedDocumentReference(id = "222-555-G-COVGENE", patient_id = "PA0222", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://555.csv"))),
    NormalizedDocumentReference(id = "222-555-G-SNV", patient_id = "PA0222", `type` = "SNV", contents = List(Content(format = "TBI", s3_url = "s3a://555.vcf.gz.tbi"))),

    // 5. Solo -- Mix of SNV VCF and SNV TBI files
    // Tumor only analysis
    NormalizedDocumentReference(id = "1111-1111-TO-SNV", patient_id = "PA1111", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://1111.vcf.gz"), Content(format = "TBI", s3_url = "s3a://1111.vcf.gz.tbi"))),
    // Germline analysis
    NormalizedDocumentReference(id = "1111-2222-G-SNV", patient_id = "PA1111", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://2222.vcf.gz"), Content(format = "TBI", s3_url = "s3a://2222.vcf.gz.tbi"))),
    // Tumor normal analysis
    NormalizedDocumentReference(id = "1111-1111-TN-SNV", patient_id = "PA1111", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://1111-2222.vcf.gz"), Content(format = "TBI", s3_url = "s3a://1111-2222.vcf.gz.tbi"))),

    // 6. API Solo WGS
    NormalizedDocumentReference(id = "API-DOC-01-SNV", patient_id = "API-PA-0001", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://api-01-snv.vcf.gz"), Content(format = "TBI", s3_url = "s3a://api-01-snv.vcf.gz.tbi"))),
  ).toDF()

  val data: Map[String, DataFrame] = Map(
    normalized_patient.id -> patientDf,
    normalized_person.id -> personDf,
    normalized_service_request.id -> serviceRequestDf,
    normalized_clinical_impression.id -> clinicalImpressionsDf,
    normalized_observation.id -> observationsDf,
    normalized_code_system.id -> codeSystemDf,
    normalized_family.id -> familyDf,
    normalized_task.id -> taskDf,
    normalized_specimen.id -> specimenDf,
    normalized_document_reference.id -> documentDf,
  )

  it should "transform normalized clinical data into a single clinical reference table" in {
    val job = EnrichedClinical(TestETLContext())
    val result = job.transformSingle(data)

    result
      .groupBy("patient_id", "analysis_id", "bioinfo_analysis_code")
      .count()
      .filter($"count" > 1)
      .isEmpty shouldBe true

    result
      .as[EnrichedClinicalOutput]
      .collect() should contain theSameElementsAs Seq(
      // 1. Trio
      // Proband
      EnrichedClinicalOutput(patient_id = "PA0001", gender = "Male", analysis_id = "SRA0001", sequencing_id = "SRS0001", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "1", specimen_id = "SP_0001", sample_id = "SA_0001", is_proband = true, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00001"), mother_id = Some("PA0003"), father_id = Some("PA0002"), mother_aliquot_id = Some("3"), father_aliquot_id = Some("2"),
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true))),
        person_id = Some("P0001"), birth_date = Some(Date.valueOf("2001-03-06")), first_name = Some("John"),
        covgene_urls = Some(Set("s3a://1.csv")), exomiser_urls = Some(Set("s3a://1.tsv")), cnv_vcf_urls = Some(Set("s3a://1-1.vcf.gz", "s3a://1-2.vcf.gz")), snv_vcf_urls = None),
      // Father
      EnrichedClinicalOutput(patient_id = "PA0002", gender = "Male", analysis_id = "SRA0001", sequencing_id = "SRS0002", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "2", specimen_id = "SP_0002", sample_id = "SA_0002", is_proband = false, affected_status = false, affected_status_code = "not_affected",
        family_id = Some("FM00001"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = None, person_id = Some("P0001"), birth_date = Some(Date.valueOf("2001-03-06")), first_name = Some("John"),
        covgene_urls = Some(Set("s3a://2.csv")), exomiser_urls = Some(Set("s3a://2.tsv")), cnv_vcf_urls = Some(Set("s3a://2-1.vcf.gz", "s3a://2-2.vcf.gz")), snv_vcf_urls = None),
      // Mother
      EnrichedClinicalOutput(patient_id = "PA0003", gender = "Female", analysis_id = "SRA0001", sequencing_id = "SRS0003", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "3", specimen_id = "SP_0003", sample_id = "SA_0003", is_proband = false, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00001"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        person_id = Some("P0002"), birth_date = Some(Date.valueOf("2001-03-06")), first_name = Some("Jane"),
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true))),
        covgene_urls = Some(Set("s3a://3.csv")), exomiser_urls = Some(Set("s3a://3.tsv")), cnv_vcf_urls = Some(Set("s3a://3-1.vcf.gz", "s3a://3-2.vcf.gz")), snv_vcf_urls = None),

      // 2. Trio+
      // Proband
      EnrichedClinicalOutput(patient_id = "PA0004", gender = "Male", analysis_id = "SRA0004", sequencing_id = "SRS0004", bioinfo_analysis = "somatic", bioinfo_analysis_code = "TEBA",
        practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "4", specimen_id = "SP_0004", sample_id = "SA_0004", is_proband = true, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00004"), mother_id = Some("PA0007"), father_id = Some("PA0006"), mother_aliquot_id = Some("7"), father_aliquot_id = Some("6"),
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true), ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "affected", affected_status = true))),
        exomiser_urls = Some(Set("s3a://4.tsv")), cnv_vcf_urls = None, snv_vcf_urls = None),
      // Brother
      EnrichedClinicalOutput(patient_id = "PA0005", gender = "Male", analysis_id = "SRA0004", sequencing_id = "SRS0005", bioinfo_analysis = "somatic", bioinfo_analysis_code = "TEBA",
        practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "5", specimen_id = "SP_0005", sample_id = "SA_0005", is_proband = false, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00004"), mother_id = Some("PA0007"), father_id = Some("PA0006"), mother_aliquot_id = Some("7"), father_aliquot_id = Some("6"),
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true), ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "not_affected", affected_status = false))),
        exomiser_urls = Some(Set("s3a://5.tsv")), cnv_vcf_urls = None, snv_vcf_urls = None),
      // Father
      EnrichedClinicalOutput(patient_id = "PA0006", gender = "Male", analysis_id = "SRA0004", sequencing_id = "SRS0006", bioinfo_analysis = "somatic", bioinfo_analysis_code = "TEBA",
        practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "6", specimen_id = "SP_0006", sample_id = "SA_0006", is_proband = false, affected_status = false, affected_status_code = "not_affected",
        family_id = Some("FM00004"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = None,
        exomiser_urls = Some(Set("s3a://6.tsv")), cnv_vcf_urls = None, snv_vcf_urls = None),
      // Mother
      EnrichedClinicalOutput(patient_id = "PA0007", gender = "Female", analysis_id = "SRA0004", sequencing_id = "SRS0007", bioinfo_analysis = "somatic", bioinfo_analysis_code = "TEBA",
        practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "7", specimen_id = "SP_0007", sample_id = "SA_0007", is_proband = false, affected_status = false, affected_status_code = "not_affected",
        family_id = Some("FM00004"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = None,
        exomiser_urls = Some(Set("s3a://7.tsv")), cnv_vcf_urls = None, snv_vcf_urls = None),

      // 3. Incomplete trio
      // Proband
      EnrichedClinicalOutput(patient_id = "PA0011", gender = "Male", analysis_id = "SRA0011", sequencing_id = "SRS0011", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00102", batch_id = "BAT2", aliquot_id = "11", specimen_id = "SP_0011", sample_id = "SA_0011", is_proband = true, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00011"), mother_id = Some("PA0022"), father_id = Some("PA0033"), mother_aliquot_id = Some("22"), father_aliquot_id = Some("33"),
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true), ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "not_affected", affected_status = false))),
        covgene_urls = Some(Set("s3a://11-1.csv", "s3a://11-2.csv", "s3a://11-3.csv")), cnv_vcf_urls = None, snv_vcf_urls = None),
      // Mother
      EnrichedClinicalOutput(patient_id = "PA0022", gender = "Female", analysis_id = "SRA0011", sequencing_id = "SRS0022", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00102", batch_id = "BAT2", aliquot_id = "22", specimen_id = "SP_0022", sample_id = "SA_0022", is_proband = false, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00011"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true), ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "not_affected", affected_status = false), ClinicalSign(id = "HP:0000003", name = "Term 3", affected_status_code = "unknown", affected_status = false))),
        covgene_urls = Some(Set("s3a://22-1.csv", "s3a://22-2.csv")), cnv_vcf_urls = None, snv_vcf_urls = None),
      // Father we receive later
      EnrichedClinicalOutput(patient_id = "PA0033", gender = "Male", analysis_id = "SRA0011", sequencing_id = "SRS0033", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00102", batch_id = "BAT3", aliquot_id = "33", specimen_id = "SP_0033", sample_id = "SA_0033", is_proband = false, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00011"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true), ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "unknown", affected_status = false), ClinicalSign(id = "HP:0000003", name = "Term 3", affected_status_code = "affected", affected_status = true))),
        covgene_urls = Some(Set("s3a://33.csv")), cnv_vcf_urls = None, snv_vcf_urls = None),

      // 4. Same family, two prescriptions
      // 4.1 First prescription: Duo
      // Proband
      EnrichedClinicalOutput(patient_id = "PA0111", gender = "Female", analysis_id = "SRA0111", sequencing_id = "SRS0111", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00102", batch_id = "BAT2", aliquot_id = "111", specimen_id = "SP_0111", sample_id = "SA_0111", is_proband = true, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00111"), mother_id = Some("PA0222"), father_id = None, mother_aliquot_id = Some("222"), father_aliquot_id = None,
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true))),
        covgene_urls = None, cnv_vcf_urls = None, snv_vcf_urls = None),
      // Mother
      EnrichedClinicalOutput(patient_id = "PA0222", gender = "Female", analysis_id = "SRA0111", sequencing_id = "SRS0222", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00102", batch_id = "BAT2", aliquot_id = "222", specimen_id = "SP_0222", sample_id = "SA_0222", is_proband = false, affected_status = false, affected_status_code = "not_affected",
        family_id = Some("FM00111"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = None,
        covgene_urls = None, cnv_vcf_urls = None, snv_vcf_urls = None),
      // 4.2 Second prescription: Trio
      // Proband
      EnrichedClinicalOutput(patient_id = "PA0333", gender = "Female", analysis_id = "SRA0333", sequencing_id = "SRS0333", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00102", batch_id = "BAT3", aliquot_id = "333", specimen_id = "SP_0333", sample_id = "SA_0333", is_proband = true, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00222"), mother_id = Some("PA0222"), father_id = None, mother_aliquot_id = Some("555"), father_aliquot_id = None,
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "affected", affected_status = true))),
        covgene_urls = Some(Set("s3a://333.csv")), exomiser_urls = Some(Set("s3a://333.tsv")), exomiser_cnv_urls = Some(Set("s3a://333_cnv.tsv")), cnv_vcf_urls = None, snv_vcf_urls = None),
      // Sister
      EnrichedClinicalOutput(patient_id = "PA0111", gender = "Female", analysis_id = "SRA0333", sequencing_id = "SRS0444", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00102", batch_id = "BAT3", aliquot_id = "111", specimen_id = "SP_0111", sample_id = "SA_0111", is_proband = false, affected_status = false, affected_status_code = "not_affected",
        family_id = Some("FM00222"), mother_id = Some("PA0222"), father_id = None, mother_aliquot_id = Some("555"), father_aliquot_id = None,
        clinical_signs = None,
        covgene_urls = None, cnv_vcf_urls = None, snv_vcf_urls = None),
      // Mother
      EnrichedClinicalOutput(patient_id = "PA0222", gender = "Female", analysis_id = "SRA0333", sequencing_id = "SRS0555", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00102", batch_id = "BAT3", aliquot_id = "555", specimen_id = "SP_0555", sample_id = "SA_0555", is_proband = false, affected_status = true, affected_status_code = "affected",
        family_id = Some("FM00222"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "affected", affected_status = true))),
        covgene_urls = Some(Set("s3a://555.csv")), cnv_vcf_urls = None, snv_vcf_urls = None),

      // 5. Solo
      // Tumor only analysis
      EnrichedClinicalOutput(patient_id = "PA1111", gender = "Female", analysis_id = "SRA1111", sequencing_id = "SRS1111", bioinfo_analysis = "somatic", bioinfo_analysis_code = "TEBA",
        practitioner_role_id = "PPR00103", batch_id = "BAT3", aliquot_id = "1111", specimen_id = "SP_1111", sample_id = "SA_1111", is_proband = true, affected_status = true, affected_status_code = "affected",
        family_id = None, mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true), ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "not_affected", affected_status = false), ClinicalSign(id = "HP:0000003", name = "Term 3", affected_status_code = "unknown", affected_status = false))),
        covgene_urls = None, cnv_vcf_urls = None, snv_vcf_urls = Some(Set("s3a://1111.vcf.gz"))),
      // Germline analysis
      EnrichedClinicalOutput(patient_id = "PA1111", gender = "Female", analysis_id = "SRA2222", sequencing_id = "SRS2222", bioinfo_analysis = "germline", bioinfo_analysis_code = "GEBA",
        practitioner_role_id = "PPR00103", batch_id = "BAT4", aliquot_id = "2222", specimen_id = "SP_2222", sample_id = "SA_2222", is_proband = true, affected_status = true, affected_status_code = "affected",
        family_id = None, mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true), ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "not_affected", affected_status = false), ClinicalSign(id = "HP:0000003", name = "Term 3", affected_status_code = "unknown", affected_status = false))),
        covgene_urls = None, cnv_vcf_urls = None, snv_vcf_urls = Some(Set("s3a://2222.vcf.gz"))),
      // Tumor normal analysis
      EnrichedClinicalOutput(patient_id = "PA1111", gender = "Female", analysis_id = "SRA1111", sequencing_id = "SRS1111", bioinfo_analysis = "somatic", bioinfo_analysis_code = "TNEBA",
        practitioner_role_id = "PPR00103", batch_id = "BAT5", aliquot_id = "1111", specimen_id = "SP_1111", sample_id = "SA_1111", is_proband = true, affected_status = true, affected_status_code = "affected",
        family_id = None, mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None,
        clinical_signs = Some(Set(ClinicalSign(id = "HP:0000001", name = "Term 1", affected_status_code = "affected", affected_status = true), ClinicalSign(id = "HP:0000002", name = "Term 2", affected_status_code = "not_affected", affected_status = false), ClinicalSign(id = "HP:0000003", name = "Term 3", affected_status_code = "unknown", affected_status = false))),
        covgene_urls = None, cnv_vcf_urls = None, snv_vcf_urls = Some(Set("s3a://1111-2222.vcf.gz"))),
    )
  }

}
