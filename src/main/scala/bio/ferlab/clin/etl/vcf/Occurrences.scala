package bio.ferlab.clin.etl.vcf

import bio.ferlab.clin.etl.vcf.Occurrences.getFamilyRelationships
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

abstract class Occurrences(batchId: String, contig: String)(implicit configuration: Configuration) extends ETL {

  def raw_variant_calling: DatasetConf

  val patient: DatasetConf = conf.getDataset("normalized_patient")
  val group: DatasetConf = conf.getDataset("normalized_group")
  val task: DatasetConf = conf.getDataset("normalized_task")
  val service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val specimen: DatasetConf = conf.getDataset("normalized_specimen")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_variant_calling.id ->
        vcf(raw_variant_calling.location.replace("{{BATCH_ID}}", batchId), referenceGenomePath = None)
          .where(s"contigName='$contig'"),
      patient.id -> patient.read,
      group.id -> group.read,
      task.id -> task.read,
      service_request.id -> service_request.read,
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
    val serviceRequestDf = data(service_request.id)
      .select(
        col("id") as "service_request_id",
        col("service_request_code") as "analysis_code",
        col("service_request_description") as "analysis_display_name"
      )
    val groupDf = data(group.id)
      .withColumn("member", explode(col("members")))
      .select(
        col("member.affected_status") as "affected_status",
        col("member.patient_id") as "patient_id"
      )

    val patients = data(patient.id)
      .select(
        col("id") as "patient_id",
        col("family_id"),
        col("is_proband"),
        col("gender"),
        col("practitioner_role_id"),
        col("organization_id")
      )
      .join(groupDf, Seq("patient_id"), "left")
      .withColumn("gender",
        when(col("gender") === "male", lit("Male"))
          .when(col("gender") === "female", lit("Female"))
          .otherwise(col("gender")))

    val familyRelationshipDf = getFamilyRelationships(data(patient.id))

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
        .join(serviceRequestDf, Seq("service_request_id"), "left")
        .join(patients, Seq("patient_id"))
        .join(familyRelationshipDf, Seq("patient_id"), "left")
        .join(specimenDf, Seq("service_request_id", "patient_id"), "left")
    joinedRelation
  }


}

object Occurrences {
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