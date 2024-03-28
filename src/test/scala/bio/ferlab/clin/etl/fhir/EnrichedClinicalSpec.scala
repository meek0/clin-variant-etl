package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.model.normalized.fhir._
import bio.ferlab.clin.model.enriched.{EnrichedClinical => EnrichedClinicalOutput}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.{ClassGenerator, SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class EnrichedClinicalSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val normalized_clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val normalized_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  val normalized_family: DatasetConf = conf.getDataset("normalized_family")
  val normalized_observation: DatasetConf = conf.getDataset("normalized_observation")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
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
  ).toDF()

  val clinicalImpressionsDf: DataFrame = Seq(
    // 1. Trio
    NormalizedClinicalImpression(id = "CI0001", `patient_id` = "PA0001", observations = List("OB0001", "OB000X")),
    NormalizedClinicalImpression(id = "CI0002", `patient_id` = "PA0002", observations = List("OB0002")),
    NormalizedClinicalImpression(id = "CI0003", `patient_id` = "PA0003", observations = List("OB0003")),

    // 2. Trio+
    NormalizedClinicalImpression(id = "CI0004", `patient_id` = "PA0004", observations = List("OB0004")),
    NormalizedClinicalImpression(id = "CI0005", `patient_id` = "PA0005", observations = List("OB0005")),
    NormalizedClinicalImpression(id = "CI0006", `patient_id` = "PA0006", observations = List("OB0006")),
    NormalizedClinicalImpression(id = "CI0007", `patient_id` = "PA0007", observations = List("OB0007")),

    // 3. Incomplete trio
    NormalizedClinicalImpression(id = "CI0011", `patient_id` = "PA0011", observations = List("OB0011")),
    NormalizedClinicalImpression(id = "CI0022", `patient_id` = "PA0022", observations = List("OB0022")),
    NormalizedClinicalImpression(id = "CI0033", `patient_id` = "PA0033", observations = List("OB0033")),

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedClinicalImpression(id = "CI0111", `patient_id` = "PA0111", observations = List("OB0111", "OB0XXX")),
    NormalizedClinicalImpression(id = "CI0222", `patient_id` = "PA0222", observations = List("OB0222", "OB0YYY", "OB0ZZZ")),
    // 4.2 Second prescription: Trio
    NormalizedClinicalImpression(id = "CI0333", `patient_id` = "PA0333", observations = List("OB0333")),
    NormalizedClinicalImpression(id = "CI0444", `patient_id` = "PA0111", observations = List("OB0444")),
    NormalizedClinicalImpression(id = "CI0555", `patient_id` = "PA0222", observations = List("OB0555")),

    // 5. Solo
    // Clinical impression for tumor only analysis
    NormalizedClinicalImpression(id = "CI1111", `patient_id` = "PA1111", observations = List("OB1111")),
    // Clinical impression for germline analysis
    NormalizedClinicalImpression(id = "CI2222", `patient_id` = "PA1111", observations = List("OB2222")),
  ).toDF()

  val observationsDf: DataFrame = Seq(
    // 1. Trio
    NormalizedObservation(id = "OB0001", patient_id = "PA0001", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB000X", patient_id = "PA0001", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0002", patient_id = "PA0002", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    NormalizedObservation(id = "OB0003", patient_id = "PA0003", `observation_code` = "DSTA", `interpretation_code` = "affected"),

    // 2. Trio+
    NormalizedObservation(id = "OB0004", patient_id = "PA0004", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0005", patient_id = "PA0005", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0006", patient_id = "PA0006", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    NormalizedObservation(id = "OB0007", patient_id = "PA0007", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),

    // 3. Incomplete trio
    NormalizedObservation(id = "OB0011", patient_id = "PA0011", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0022", patient_id = "PA0022", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0033", patient_id = "PA0033", `observation_code` = "DSTA", `interpretation_code` = "affected"), // Received later

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedObservation(id = "OB0111", patient_id = "PA0111", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0222", patient_id = "PA0222", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    NormalizedObservation(id = "OB0XXX", patient_id = "PA0111", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0YYY", patient_id = "PA0222", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0ZZZ", patient_id = "PA0222", `observation_code` = "OTHER", `interpretation_code` = "affected"),
    // 4.2 Second prescription: Trio
    NormalizedObservation(id = "OB0333", patient_id = "PA0333", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    NormalizedObservation(id = "OB0444", patient_id = "PA0111", `observation_code` = "DSTA", `interpretation_code` = "not_affected"),
    NormalizedObservation(id = "OB0555", patient_id = "PA0222", `observation_code` = "DSTA", `interpretation_code` = "affected"),

    // 5. Solo
    // Observation for tumor only analysis
    NormalizedObservation(id = "OB1111", patient_id = "PA1111", `observation_code` = "DSTA", `interpretation_code` = "affected"),
    // Observation for germline analysis
    NormalizedObservation(id = "OB2222", patient_id = "PA1111", `observation_code` = "DSTA", `interpretation_code` = "affected"),
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
    // BATCH 1
    // 1. Trio, germline
    NormalizedTask(batch_id = "BAT1", `patient_id` = "PA0001", `service_request_id` = "SRS0001", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "1")),
    NormalizedTask(batch_id = "BAT1", `patient_id` = "PA0002", `service_request_id` = "SRS0002", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "2")),
    NormalizedTask(batch_id = "BAT1", `patient_id` = "PA0003", `service_request_id` = "SRS0003", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "3")),

    // 2. Trio+, tumor only
    NormalizedTask(batch_id = "BAT1", `patient_id` = "PA0004", `service_request_id` = "SRS0004", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "4")),
    NormalizedTask(batch_id = "BAT1", `patient_id` = "PA0005", `service_request_id` = "SRS0005", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "5")),
    NormalizedTask(batch_id = "BAT1", `patient_id` = "PA0006", `service_request_id` = "SRS0006", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "6")),
    NormalizedTask(batch_id = "BAT1", `patient_id` = "PA0007", `service_request_id` = "SRS0007", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "7")),

    // BATCH 2
    // 3. Incomplete trio
    NormalizedTask(batch_id = "BAT2", `patient_id` = "PA0011", `service_request_id` = "SRS0011", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "11")), // proband
    NormalizedTask(batch_id = "BAT2", `patient_id` = "PA0022", `service_request_id` = "SRS0022", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "22")), // mother

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedTask(batch_id = "BAT2", `patient_id` = "PA0111", `service_request_id` = "SRS0111", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "111")),
    NormalizedTask(batch_id = "BAT2", `patient_id` = "PA0222", `service_request_id` = "SRS0222", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "222")),

    // BATCH 3
    // 3. Incomplete trio, father's task
    NormalizedTask(batch_id = "BAT3", `patient_id` = "PA0033", `service_request_id` = "SRS0033", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "33")), // father

    // 4. Same family, two prescriptions
    // 4.2 Second prescription: Trio
    NormalizedTask(batch_id = "BAT3", `patient_id` = "PA0333", `service_request_id` = "SRS0333", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "333")), // proband
    NormalizedTask(batch_id = "BAT3", `patient_id` = "PA0111", `service_request_id` = "SRS0444", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "111")), // sister (same aliquot since she's not re-sequenced)
    NormalizedTask(batch_id = "BAT3", `patient_id` = "PA0222", `service_request_id` = "SRS0555", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "555")), // mother

    // 5. Solo, Tumor only analysis
    NormalizedTask(batch_id = "BAT3", `patient_id` = "PA1111", `service_request_id` = "SRS1111", `analysis_code` = "TEBA", `experiment` = EXPERIMENT(`aliquot_id` = "1111")),

    // BATCH 4
    // 5. Solo, Germline analysis
    NormalizedTask(batch_id = "BAT4", `patient_id` = "PA1111", `service_request_id` = "SRS2222", `analysis_code` = "GEAN", `experiment` = EXPERIMENT(`aliquot_id` = "2222")),

    // BATCH 5
    // 5. Solo, tumor normal (same servie_request_id and aliquot_id as tumor only)
    NormalizedTask(batch_id = "BAT5", `patient_id` = "PA1111", `service_request_id` = "SRS1111", `analysis_code` = "TNEBA", `experiment` = EXPERIMENT(`aliquot_id` = "1111")),
  ).toDF()

  val specimenDf: DataFrame = Seq(
    // 1. Trio
    NormalizedSpecimen(`patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = Some("SA_0001"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0001", `service_request_id` = "SRS0001", `sample_id` = None, `specimen_id` = Some("SP_0001")),
    NormalizedSpecimen(`patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = Some("SA_0002"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0002", `service_request_id` = "SRS0002", `sample_id` = None, `specimen_id` = Some("SP_0002")),
    NormalizedSpecimen(`patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = Some("SA_0003"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0003", `service_request_id` = "SRS0003", `sample_id` = None, `specimen_id` = Some("SP_0003")),

    // 2. Trio+
    NormalizedSpecimen(`patient_id` = "PA0004", `service_request_id` = "SRS0004", `sample_id` = Some("SA_0004"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0004", `service_request_id` = "SRS0004", `sample_id` = None, `specimen_id` = Some("SP_0004")),
    NormalizedSpecimen(`patient_id` = "PA0005", `service_request_id` = "SRS0005", `sample_id` = Some("SA_0005"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0005", `service_request_id` = "SRS0005", `sample_id` = None, `specimen_id` = Some("SP_0005")),
    NormalizedSpecimen(`patient_id` = "PA0006", `service_request_id` = "SRS0006", `sample_id` = Some("SA_0006"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0006", `service_request_id` = "SRS0006", `sample_id` = None, `specimen_id` = Some("SP_0006")),
    NormalizedSpecimen(`patient_id` = "PA0007", `service_request_id` = "SRS0007", `sample_id` = Some("SA_0007"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0007", `service_request_id` = "SRS0007", `sample_id` = None, `specimen_id` = Some("SP_0007")),

    // 3. Incomplete trio
    NormalizedSpecimen(`patient_id` = "PA0011", `service_request_id` = "SRS0011", `sample_id` = Some("SA_0011"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0011", `service_request_id` = "SRS0011", `sample_id` = None, `specimen_id` = Some("SP_0011")),
    NormalizedSpecimen(`patient_id` = "PA0022", `service_request_id` = "SRS0022", `sample_id` = Some("SA_0022"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0022", `service_request_id` = "SRS0022", `sample_id` = None, `specimen_id` = Some("SP_0022")),
    NormalizedSpecimen(`patient_id` = "PA0033", `service_request_id` = "SRS0033", `sample_id` = Some("SA_0033"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0033", `service_request_id` = "SRS0033", `sample_id` = None, `specimen_id` = Some("SP_0033")),

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo
    NormalizedSpecimen(`patient_id` = "PA0111", `service_request_id` = "SRS0111", `sample_id` = Some("SA_0111"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0111", `service_request_id` = "SRS0111", `sample_id` = None, `specimen_id` = Some("SP_0111")),
    NormalizedSpecimen(`patient_id` = "PA0222", `service_request_id` = "SRS0222", `sample_id` = Some("SA_0222"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0222", `service_request_id` = "SRS0222", `sample_id` = None, `specimen_id` = Some("SP_0222")),

    // 4.2 Second prescription: Trio
    NormalizedSpecimen(`patient_id` = "PA0333", `service_request_id` = "SRS0333", `sample_id` = Some("SA_0333"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0333", `service_request_id` = "SRS0333", `sample_id` = None, `specimen_id` = Some("SP_0333")),
    NormalizedSpecimen(`patient_id` = "PA0111", `service_request_id` = "SRS0444", `sample_id` = Some("SA_0111"), `specimen_id` = None), // Same sample_id and specimen_id as past sequencing
    NormalizedSpecimen(`patient_id` = "PA0111", `service_request_id` = "SRS0444", `sample_id` = None, `specimen_id` = Some("SP_0111")),
    NormalizedSpecimen(`patient_id` = "PA0222", `service_request_id` = "SRS0555", `sample_id` = Some("SA_0555"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA0222", `service_request_id` = "SRS0555", `sample_id` = None, `specimen_id` = Some("SP_0555")),

    // 5. Solo
    // Tumor only analysis
    NormalizedSpecimen(`patient_id` = "PA1111", `service_request_id` = "SRS1111", `sample_id` = Some("SA_1111"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA1111", `service_request_id` = "SRS1111", `sample_id` = None, `specimen_id` = Some("SP_1111")),
    // Germline analysis
    NormalizedSpecimen(`patient_id` = "PA1111", `service_request_id` = "SRS2222", `sample_id` = Some("SA_2222"), `specimen_id` = None),
    NormalizedSpecimen(`patient_id` = "PA1111", `service_request_id` = "SRS2222", `sample_id` = None, `specimen_id` = Some("SP_2222")),
  ).toDF

  val documentDf: DataFrame = Seq(
    // 1. Trio: Everyone with Covgene and Exomiser
    NormalizedDocumentReference(patient_id = "PA0001", specimen_id = "SP_0001",`type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://1.csv"))),
    NormalizedDocumentReference(patient_id = "PA0001", specimen_id = "SP_0001",`type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://1.tsv"), Content(format = "JSON", s3_url = "s3a://1.json"))),
    NormalizedDocumentReference(patient_id = "PA0002", specimen_id = "SP_0002",`type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://2.csv"))),
    NormalizedDocumentReference(patient_id = "PA0002", specimen_id = "SP_0002",`type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://2.tsv"), Content(format = "JSON", s3_url = "s3a://2.json"))),
    NormalizedDocumentReference(patient_id = "PA0003", specimen_id = "SP_0003",`type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://3.csv"))),
    NormalizedDocumentReference(patient_id = "PA0003", specimen_id = "SP_0003",`type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://3.tsv"), Content(format = "JSON", s3_url = "s3a://3.json"))),

    // 2. Trio+: Everyone with Exomiser
    NormalizedDocumentReference(patient_id = "PA0004", specimen_id = "SP_0004", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://4.tsv"), Content(format = "JSON", s3_url = "s3a://4.json"))),
    NormalizedDocumentReference(patient_id = "PA0005", specimen_id = "SP_0005", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://5.tsv"), Content(format = "JSON", s3_url = "s3a://5.json"))),
    NormalizedDocumentReference(patient_id = "PA0006", specimen_id = "SP_0006", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://6.tsv"), Content(format = "JSON", s3_url = "s3a://6.json"))),
    NormalizedDocumentReference(patient_id = "PA0007", specimen_id = "SP_0007", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://7.tsv"), Content(format = "JSON", s3_url = "s3a://7.json"))),

    // 3. Incomplete trio: Multiple Covgene files per specimen
    NormalizedDocumentReference(patient_id = "PA0011", specimen_id = "SP_0011",`type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://11-1.csv"), Content(format = "CSV", s3_url = "s3a://11-2.csv"), Content(format = "CSV", s3_url = "s3a://11-3.csv"))),
    NormalizedDocumentReference(patient_id = "PA0022", specimen_id = "SP_0022",`type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://22-1.csv"), Content(format = "CSV", s3_url = "s3a://22-2.csv"))),
    NormalizedDocumentReference(patient_id = "PA0033", specimen_id = "SP_0033",`type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://33.csv"))),

    // 4. Same family, two prescriptions
    // 4.1 First prescription: Duo -- Only SNV files that should not appear in table
    NormalizedDocumentReference(patient_id = "PA0111", specimen_id = "SP_0111", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://111-1.vcf"))),
    NormalizedDocumentReference(patient_id = "PA0222", specimen_id = "SP_0222", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://222.vcf"))),
    // 4.2 Second prescription: Trio -- Mix of files
    NormalizedDocumentReference(patient_id = "PA0333", specimen_id = "SP_0333", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://333.csv"))),
    NormalizedDocumentReference(patient_id = "PA0333", specimen_id = "SP_0333", `type` = "EXOMISER", contents = List(Content(format = "TSV", s3_url = "s3a://333.tsv"), Content(format = "JSON", s3_url = "s3a://333.json"))),
    NormalizedDocumentReference(patient_id = "PA0333", specimen_id = "SP_0333", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://333.vcf"))),
    NormalizedDocumentReference(patient_id = "PA0111", specimen_id = "SP_0111", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://111-2.vcf"))), // Same file as before but in different path
    NormalizedDocumentReference(patient_id = "PA0222", specimen_id = "SP_0555", `type` = "COVGENE", contents = List(Content(format = "CSV", s3_url = "s3a://555.csv"))),
    NormalizedDocumentReference(patient_id = "PA0222", specimen_id = "SP_0555", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://555.vcf"))),

    // 5. Solo -- Only SNV files that should not appear in table
    // Tumor only analysis
    NormalizedDocumentReference(patient_id = "PA1111", specimen_id = "SP_1111", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://1111.vcf"))),
    // Germline analysis
    NormalizedDocumentReference(patient_id = "PA1111", specimen_id = "SP_2222", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://2222.vcf"))),
    // Tumor normal analysis
    NormalizedDocumentReference(patient_id = "PA1111", specimen_id = "SP_1111", `type` = "SNV", contents = List(Content(format = "VCF", s3_url = "s3a://1111-2222.vcf"))),
  ).toDF()

  val data: Map[String, DataFrame] = Map(
    normalized_patient.id -> patientDf,
    normalized_service_request.id -> serviceRequestDf,
    normalized_clinical_impression.id -> clinicalImpressionsDf,
    normalized_observation.id -> observationsDf,
    normalized_family.id -> familyDf,
    normalized_task.id -> taskDf,
    normalized_specimen.id -> specimenDf,
    normalized_document_reference.id -> documentDf
  )

  it should "transform normalized clinical data into a single clinical reference table" in {
    val job = EnrichedClinical(TestETLContext())
    val result = job.transformSingle(data)

    result
      .groupBy("patient_id", "analysis_service_request_id", "bioinfo_analysis_code")
      .count()
      .filter($"count" > 1)
      .isEmpty shouldBe true

    result
      .as[EnrichedClinicalOutput]
      .collect() should contain theSameElementsAs Seq(
      // 1. Trio
      EnrichedClinicalOutput(patient_id = "PA0001", gender = "Male", analysis_service_request_id = "SRA0001", service_request_id = "SRS0001", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "1", specimen_id = "SP_0001", sample_id = "SA_0001", is_proband = true, affected_status = true, affected_status_code = "affected", family_id = Some("FM00001"), mother_id = Some("PA0003"), father_id = Some("PA0002"), mother_aliquot_id = Some("3"), father_aliquot_id = Some("2"), covgene_urls = Some(Set("s3a://1.csv")), exomiser_urls = Some(Set("s3a://1.tsv"))),
      EnrichedClinicalOutput(patient_id = "PA0002", gender = "Male", analysis_service_request_id = "SRA0001", service_request_id = "SRS0002", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "2", specimen_id = "SP_0002", sample_id = "SA_0002", is_proband = false, affected_status = false, affected_status_code = "not_affected", family_id = Some("FM00001"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None, covgene_urls = Some(Set("s3a://2.csv")), exomiser_urls = Some(Set("s3a://2.tsv"))),
      EnrichedClinicalOutput(patient_id = "PA0003", gender = "Female", analysis_service_request_id = "SRA0001", service_request_id = "SRS0003", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "3", specimen_id = "SP_0003", sample_id = "SA_0003", is_proband = false, affected_status = true, affected_status_code = "affected", family_id = Some("FM00001"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None, covgene_urls = Some(Set("s3a://3.csv")), exomiser_urls = Some(Set("s3a://3.tsv"))),

      // 2. Trio+
      EnrichedClinicalOutput(patient_id = "PA0004", gender = "Male", analysis_service_request_id = "SRA0004", service_request_id = "SRS0004", bioinfo_analysis_code = "TEBA", practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "4", specimen_id = "SP_0004", sample_id = "SA_0004", is_proband = true, affected_status = true, affected_status_code = "affected", family_id = Some("FM00004"), mother_id = Some("PA0007"), father_id = Some("PA0006"), mother_aliquot_id = Some("7"), father_aliquot_id = Some("6"), exomiser_urls = Some(Set("s3a://4.tsv"))),
      EnrichedClinicalOutput(patient_id = "PA0005", gender = "Male", analysis_service_request_id = "SRA0004", service_request_id = "SRS0005", bioinfo_analysis_code = "TEBA", practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "5", specimen_id = "SP_0005", sample_id = "SA_0005", is_proband = false, affected_status = true, affected_status_code = "affected", family_id = Some("FM00004"), mother_id = Some("PA0007"), father_id = Some("PA0006"), mother_aliquot_id = Some("7"), father_aliquot_id = Some("6"), exomiser_urls = Some(Set("s3a://5.tsv"))),
      EnrichedClinicalOutput(patient_id = "PA0006", gender = "Male", analysis_service_request_id = "SRA0004", service_request_id = "SRS0006", bioinfo_analysis_code = "TEBA", practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "6", specimen_id = "SP_0006", sample_id = "SA_0006", is_proband = false, affected_status = false, affected_status_code = "not_affected", family_id = Some("FM00004"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None, exomiser_urls = Some(Set("s3a://6.tsv"))),
      EnrichedClinicalOutput(patient_id = "PA0007", gender = "Female", analysis_service_request_id = "SRA0004", service_request_id = "SRS0007", bioinfo_analysis_code = "TEBA", practitioner_role_id = "PPR00101", batch_id = "BAT1", aliquot_id = "7", specimen_id = "SP_0007", sample_id = "SA_0007", is_proband = false, affected_status = false, affected_status_code = "not_affected", family_id = Some("FM00004"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None, exomiser_urls = Some(Set("s3a://7.tsv"))),

      // 3. Incomplete trio
      EnrichedClinicalOutput(patient_id = "PA0011", gender = "Male", analysis_service_request_id = "SRA0011", service_request_id = "SRS0011", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00102", batch_id = "BAT2", aliquot_id = "11", specimen_id = "SP_0011", sample_id = "SA_0011", is_proband = true, affected_status = true, affected_status_code = "affected", family_id = Some("FM00011"), mother_id = Some("PA0022"), father_id = Some("PA0033"), mother_aliquot_id = Some("22"), father_aliquot_id = Some("33"), covgene_urls = Some(Set("s3a://11-1.csv", "s3a://11-2.csv", "s3a://11-3.csv"))),
      EnrichedClinicalOutput(patient_id = "PA0022", gender = "Female", analysis_service_request_id = "SRA0011", service_request_id = "SRS0022", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00102", batch_id = "BAT2", aliquot_id = "22", specimen_id = "SP_0022", sample_id = "SA_0022", is_proband = false, affected_status = true, affected_status_code = "affected", family_id = Some("FM00011"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None, covgene_urls = Some(Set("s3a://22-1.csv", "s3a://22-2.csv"))),
      EnrichedClinicalOutput(patient_id = "PA0033", gender = "Male", analysis_service_request_id = "SRA0011", service_request_id = "SRS0033", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00102", batch_id = "BAT3", aliquot_id = "33", specimen_id = "SP_0033", sample_id = "SA_0033", is_proband = false, affected_status = true, affected_status_code = "affected", family_id = Some("FM00011"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None, covgene_urls = Some(Set("s3a://33.csv"))),

      // 4. Same family, two prescriptions
      // 4.1 First prescription: Duo
      EnrichedClinicalOutput(patient_id = "PA0111", gender = "Female", analysis_service_request_id = "SRA0111", service_request_id = "SRS0111", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00102", batch_id = "BAT2", aliquot_id = "111", specimen_id = "SP_0111", sample_id = "SA_0111", is_proband = true, affected_status = true, affected_status_code = "affected", family_id = Some("FM00111"), mother_id = Some("PA0222"), father_id = None, mother_aliquot_id = Some("222"), father_aliquot_id = None),
      EnrichedClinicalOutput(patient_id = "PA0222", gender = "Female", analysis_service_request_id = "SRA0111", service_request_id = "SRS0222", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00102", batch_id = "BAT2", aliquot_id = "222", specimen_id = "SP_0222", sample_id = "SA_0222", is_proband = false, affected_status = false, affected_status_code = "not_affected", family_id = Some("FM00111"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None),
      // 4.2 Second prescription: Trio
      EnrichedClinicalOutput(patient_id = "PA0333", gender = "Female", analysis_service_request_id = "SRA0333", service_request_id = "SRS0333", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00102", batch_id = "BAT3", aliquot_id = "333", specimen_id = "SP_0333", sample_id = "SA_0333", is_proband = true, affected_status = true, affected_status_code = "affected", family_id = Some("FM00222"), mother_id = Some("PA0222"), father_id = None, mother_aliquot_id = Some("555"), father_aliquot_id = None, covgene_urls = Some(Set("s3a://333.csv")), exomiser_urls = Some(Set("s3a://333.tsv"))),
      EnrichedClinicalOutput(patient_id = "PA0111", gender = "Female", analysis_service_request_id = "SRA0333", service_request_id = "SRS0444", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00102", batch_id = "BAT3", aliquot_id = "111", specimen_id = "SP_0111", sample_id = "SA_0111", is_proband = false, affected_status = false, affected_status_code = "not_affected", family_id = Some("FM00222"), mother_id = Some("PA0222"), father_id = None, mother_aliquot_id = Some("555"), father_aliquot_id = None),
      EnrichedClinicalOutput(patient_id = "PA0222", gender = "Female", analysis_service_request_id = "SRA0333", service_request_id = "SRS0555", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00102", batch_id = "BAT3", aliquot_id = "555", specimen_id = "SP_0555", sample_id = "SA_0555", is_proband = false, affected_status = true, affected_status_code = "affected", family_id = Some("FM00222"), mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None, covgene_urls = Some(Set("s3a://555.csv"))),

      // 5. Solo
      // Tumor only analysis
      EnrichedClinicalOutput(patient_id = "PA1111", gender = "Female", analysis_service_request_id = "SRA1111", service_request_id = "SRS1111", bioinfo_analysis_code = "TEBA", practitioner_role_id = "PPR00103", batch_id = "BAT3", aliquot_id = "1111", specimen_id = "SP_1111", sample_id = "SA_1111", is_proband = true, affected_status = true, affected_status_code = "affected", family_id = None, mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None),
      // Germline analysis
      EnrichedClinicalOutput(patient_id = "PA1111", gender = "Female", analysis_service_request_id = "SRA2222", service_request_id = "SRS2222", bioinfo_analysis_code = "GEAN", practitioner_role_id = "PPR00103", batch_id = "BAT4", aliquot_id = "2222", specimen_id = "SP_2222", sample_id = "SA_2222", is_proband = true, affected_status = true, affected_status_code = "affected", family_id = None, mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None),
      // Tumor normal analysis
      EnrichedClinicalOutput(patient_id = "PA1111", gender = "Female", analysis_service_request_id = "SRA1111", service_request_id = "SRS1111", bioinfo_analysis_code = "TNEBA", practitioner_role_id = "PPR00103", batch_id = "BAT5", aliquot_id = "1111", specimen_id = "SP_1111", sample_id = "SA_1111", is_proband = true, affected_status = true, affected_status_code = "affected", family_id = None, mother_id = None, father_id = None, mother_aliquot_id = None, father_aliquot_id = None),
    )
  }

}
