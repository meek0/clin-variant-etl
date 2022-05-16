package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.vcf.Occurrences.getDiseaseStatus
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.transformation.Implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.LocalDateTime

abstract class Occurrences(batchId: String)(implicit configuration: Configuration) extends ETL {

  def raw_variant_calling: DatasetConf

  val patient: DatasetConf = conf.getDataset("normalized_patient")
  val group: DatasetConf = conf.getDataset("normalized_group")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val observation: DatasetConf = conf.getDataset("normalized_observation")
  val specimen: DatasetConf = conf.getDataset("normalized_specimen")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_variant_calling.id -> vcf(raw_variant_calling.location.replace("{{BATCH_ID}}", batchId), referenceGenomePath = None)
        .where(col("contigName").isin(validContigNames: _*)),
      patient.id -> patient.read,
      group.id -> group.read,
      task.id -> task.read,
      service_request.id -> service_request.read,
      clinical_impression.id -> clinical_impression.read,
      observation.id -> observation.read,
      specimen.id -> specimen.read
    )
  }

  def getClinicalRelation(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
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
    val familyRelationshipDf = analysisServiceRequestDf.select(
      col("id") as "analysis_service_request_id", col("patient_id"),
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
      .select(
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
        col("family") as "family",
        col("family_id") as "family_id",
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
        when(col("interpretation_code") === "POS", true).otherwise(false) as "affected_status"
      )

    val diseaseStatusByCI = clinicalImpressionDf.join(diseaseStatusDf, array_contains(col("observations"), diseaseStatusDf("observation_id")))
      .groupBy(col("clinical_impression_id"), clinicalImpressionDf("patient_id"))
      .agg(first("affected_status") as "affected_status")
    val analysisServiceRequestWithDiseaseStatus = analysisServiceRequestsDf
      .withColumn("clinical_impression_id", explode(col("clinical_impressions")))
      .join(diseaseStatusByCI, Seq("clinical_impression_id"))
      .withColumn("is_proband", diseaseStatusByCI("patient_id") === analysisServiceRequestsDf("patient_id"))
      .groupBy(diseaseStatusByCI("patient_id"), col("analysis_service_request_id"))
      .agg(
        first("affected_status") as "affected_status",
        first(analysisServiceRequestsDf("family_id")) as "family_id",
        first("is_proband") as "is_proband"
      )
    analysisServiceRequestWithDiseaseStatus
  }

  def getDistinctGroup(groupsDf: DataFrame) = {
    groupsDf
      .withColumn("member", explode(col("members")))
      .select(
        col("member.affected_status") as "affected_status",
        col("member.patient_id") as "patient_id",
        col("version_id")
      )
      .dropDuplicates(Seq("patient_id"), col("version_id").desc) //keeps only latest version of the group
      .drop("version_id")
  }

  def getFamilyRelationships(patientDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    patientDf
      .withColumn("fr", explode(col("family_relationship")))
      .select(
        $"id" as "patient1",
        $"fr.patient2" as "patient2",
        $"fr.patient1_to_patient2_relation" as "patient1_to_patient2_relation"
      ).filter($"patient1_to_patient2_relation".isin("MTH", "FTH"))
      .groupBy("patient1")
      .agg(
        map_from_entries(
          collect_list(
            struct(
              $"patient1_to_patient2_relation" as "relation",
              $"patient2" as "patient_id"
            )
          )
        ) as "relations"
      )
      .select(
        $"patient1" as "patient_id",
        $"relations.MTH" as "mother_id",
        $"relations.FTH" as "father_id"
      )
  }
}