package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.normalized.Occurrences.getDiseaseStatus
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.Logger

import java.time.LocalDateTime

abstract class Occurrences(rc: DeprecatedRuntimeETLContext, batchId: String) extends SingleETL(rc) {

  def raw_variant_calling: DatasetConf

  implicit val logger: Logger = log

  val patient: DatasetConf = conf.getDataset("normalized_patient")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val family: DatasetConf = conf.getDataset("normalized_family")
  val clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val observation: DatasetConf = conf.getDataset("normalized_observation")
  val specimen: DatasetConf = conf.getDataset("normalized_specimen")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
   Map(
      raw_variant_calling.id -> vcf(raw_variant_calling.location.replace("{{BATCH_ID}}", batchId), None, optional = true),
      patient.id -> patient.read,
      task.id -> task.read,
      service_request.id -> service_request.read,
      clinical_impression.id -> clinical_impression.read,
      family.id -> family.read,
      observation.id -> observation.read,
      specimen.id -> specimen.read
    )
  }

  def getClinicalRelation(data: Map[String, DataFrame]): DataFrame = {
    val specimenDf = data(specimen.id)
      .groupBy("service_request_id", "patient_id")
      .agg(
        filter(collect_list(col("specimen_id")), _.isNotNull)(0) as "specimen_id",
        filter(collect_list(col("sample_id")), _.isNotNull)(0) as "sample_id"
      )

    val sequencingServiceRequestDf = data(service_request.id)
      .where(col("service_request_type") === "sequencing")
      .select(
        col("id") as "service_request_id",
        col("service_request_code") as "analysis_code",
        col("service_request_description") as "analysis_display_name",
        col("analysis_service_request_id")
      )

    val analysisServiceRequestDf = data(service_request.id)
      .where(col("service_request_type") === "analysis")

    val analysisServiceRequestWithDiseaseStatus = getDiseaseStatus(analysisServiceRequestDf, data(clinical_impression.id), data(observation.id))
    val familyRelationshipDf = data(family.id).select(
      col("analysis_service_request_id"), col("patient_id"), col("family_id"),
      col("family.mother") as "mother_id", col("family.father") as "father_id"
    )

    val patients = data(patient.id)
      .select(
        col("id") as "patient_id",
        col("gender"),
        col("practitioner_role_id"),
        col("organization_id")
      )
      .withColumn("gender",
        when(col("gender") === "male", lit("Male"))
          .when(col("gender") === "female", lit("Female"))
          .otherwise(col("gender")))

    val taskDf = data(task.id)
      .where(col("batch_id") === batchId)
      .select(
        col("analysis_code") as "bioinfo_analysis_code",
        col("experiment.aliquot_id") as "aliquot_id",
        col("experiment.sequencing_strategy") as "sequencing_strategy",
        col("workflow.genome_build") as "genome_build",
        col("patient_id"),
        col("service_request_id")
      ).dropDuplicates("aliquot_id", "patient_id")

    val joinedRelation =
      taskDf
        .join(sequencingServiceRequestDf, Seq("service_request_id"))
        .join(analysisServiceRequestWithDiseaseStatus, Seq("analysis_service_request_id", "patient_id"))
        .join(patients, Seq("patient_id"))
        .join(familyRelationshipDf, Seq("analysis_service_request_id", "patient_id"), "left")
        .join(specimenDf, Seq("service_request_id", "patient_id"), "left")

    joinedRelation
  }


}

object Occurrences {

  def getDiseaseStatus(analysisServiceRequests: Dataset[Row], clinicalImpressions: DataFrame, observations: DataFrame): DataFrame = {
    val analysisServiceRequestsDf = analysisServiceRequests
      .select(
        col("id") as "analysis_service_request_id",
        col("patient_id") as "patient_id",
        col("clinical_impressions") as "clinical_impressions"
      )

    val clinicalImpressionDf = clinicalImpressions
      .select(
        col("id") as "clinical_impression_id",
        col("patient_id") as "patient_id",
        col("observations")
      )

    val diseaseStatusDf = observations
      .where(col("observation_code") === "DSTA")
      .select(
        col("id") as "observation_id",
        when(col("interpretation_code") === "affected", true).otherwise(false) as "affected_status",
        col("interpretation_code") as "affected_status_code"
      )

    val diseaseStatusByCI = clinicalImpressionDf.join(diseaseStatusDf, array_contains(col("observations"), diseaseStatusDf("observation_id")))
      .groupBy(col("clinical_impression_id"), clinicalImpressionDf("patient_id"))
      .agg(
        first("affected_status") as "affected_status",
        first("affected_status_code") as "affected_status_code"
      )
    val analysisServiceRequestWithDiseaseStatus = analysisServiceRequestsDf
      .withColumn("clinical_impression_id", explode(col("clinical_impressions")))
      .join(diseaseStatusByCI, Seq("clinical_impression_id"))
      .withColumn("is_proband", diseaseStatusByCI("patient_id") === analysisServiceRequestsDf("patient_id"))
      .groupBy(diseaseStatusByCI("patient_id"), col("analysis_service_request_id"))
      .agg(
        first("affected_status") as "affected_status",
        first("affected_status_code") as "affected_status_code",
        first("is_proband") as "is_proband"
      )
    analysisServiceRequestWithDiseaseStatus
  }



}