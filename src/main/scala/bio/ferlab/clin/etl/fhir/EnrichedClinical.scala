package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.EnrichedClinical._
import bio.ferlab.clin.etl.fhir.GenomicFile.{COVGENE, EXOMISER}
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.SparkUtils.firstAs
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime

case class EnrichedClinical(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {
  override val mainDestination: DatasetConf = conf.getDataset("enriched_clinical")
  val normalized_clinical_impression: DatasetConf = conf.getDataset("normalized_clinical_impression")
  val normalized_document_reference: DatasetConf = conf.getDataset("normalized_document_reference")
  val normalized_family: DatasetConf = conf.getDataset("normalized_family")
  val normalized_observation: DatasetConf = conf.getDataset("normalized_observation")
  val normalized_patient: DatasetConf = conf.getDataset("normalized_patient")
  val normalized_service_request: DatasetConf = conf.getDataset("normalized_service_request")
  val normalized_specimen: DatasetConf = conf.getDataset("normalized_specimen")
  val normalized_task: DatasetConf = conf.getDataset("normalized_task")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] = {
    Map(
      normalized_clinical_impression.id -> normalized_clinical_impression.read,
      normalized_document_reference.id -> normalized_document_reference.read,
      normalized_family.id -> normalized_family.read,
      normalized_observation.id -> normalized_observation.read,
      normalized_patient.id -> normalized_patient.read,
      normalized_service_request.id -> normalized_service_request.read,
      normalized_specimen.id -> normalized_specimen.read,
      normalized_task.id -> normalized_task.read,
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    val tasks = data(normalized_task.id)
      .select(
        $"patient_id",
        $"batch_id",
        $"service_request_id",
        $"analysis_code" as "bioinfo_analysis_code",
        $"experiment.aliquot_id" as "aliquot_id",
        $"experiment.sequencing_strategy" as "sequencing_strategy",
        $"workflow.genome_build" as "genome_build",
      )
      // Group by bioinfo_analysis_code since TEBA and TNEBA share same service_request_id
      .groupBy("patient_id", "service_request_id", "bioinfo_analysis_code")
      .agg(
        firstAs("batch_id"),
        firstAs("sequencing_strategy"),
        firstAs("aliquot_id"),
        firstAs("genome_build"),
      )

    val patients = data(normalized_patient.id)
      .select(
        $"id" as "patient_id",
        $"gender",
        $"practitioner_role_id",
        $"organization_id"
      )
      .withColumn("gender", when($"gender" === "male", lit("Male"))
        .when($"gender" === "female", lit("Female"))
        .otherwise($"gender"))

    val familyRelationships = data(normalized_family.id)
      .select(
        $"analysis_service_request_id",
        $"patient_id",
        $"family_id",
        $"family.mother" as "mother_id",
        $"family.father" as "father_id"
      )

    val sequencingServiceRequests = data(normalized_service_request.id)
      .where($"service_request_type" === "sequencing")
      .select(
        $"id" as "service_request_id",
        $"service_request_code" as "analysis_code",
        $"service_request_description" as "analysis_display_name",
        $"analysis_service_request_id"
      )

    val analysisServiceRequestsWithAffectedStatus = data(normalized_service_request.id)
      .where($"service_request_type" === "analysis")
      .select(
        $"id" as "analysis_service_request_id",
        $"patient_id",
        $"clinical_impressions"
      )
      .withAffectedStatus(
        clinicalImpressions = data(normalized_clinical_impression.id),
        observations = data(normalized_observation.id)
      )

    val specimensWithDocuments = data(normalized_specimen.id)
      .select(
        $"id" as "fhir_specimen_id",
        $"patient_id",
        $"service_request_id",
        $"specimen_id",
        $"sample_id"
      )
      .groupBy("patient_id", "service_request_id")
      .agg(
        collect_set($"fhir_specimen_id") as "fhir_specimen_ids",
        filter(collect_list($"specimen_id"), _.isNotNull)(0) as "specimen_id",
        filter(collect_list($"sample_id"), _.isNotNull)(0) as "sample_id"
      )
      .withDocuments(data(normalized_document_reference.id))

    tasks
      .join(sequencingServiceRequests, "service_request_id")
      .join(analysisServiceRequestsWithAffectedStatus, Seq("analysis_service_request_id", "patient_id"))
      .join(patients, "patient_id")
      .join(familyRelationships, Seq("analysis_service_request_id", "patient_id"), "left")
      .withParentAliquotIds // Needs to be done after tasks and familyRelationships join
      .join(specimensWithDocuments, Seq("service_request_id", "patient_id"), "left")
  }
}

object EnrichedClinical {
  final val GenomicFiles: List[GenomicFile] = List(COVGENE, EXOMISER)

  implicit class DataFrameOps(df: DataFrame)(implicit spark: SparkSession) {

    import spark.implicits._

    def withAffectedStatus(clinicalImpressions: DataFrame, observations: DataFrame): DataFrame = {
      val affectedStatus = observations
        .where($"observation_code" === "DSTA") // DSTA means Disease STAtus
        .select(
          $"id" as "observation_id",
          $"interpretation_code" as "affected_status_code"
        )
        .withColumn("affected_status",
          when($"affected_status_code" === "affected", true).otherwise(false))

      val affectedStatusByClinicalImpression = clinicalImpressions
        .select(
          $"id" as "clinical_impression_id",
          $"patient_id",
          explode($"observations") as "observation_id"
        )
        .join(affectedStatus, "observation_id")
        .groupBy($"clinical_impression_id", $"patient_id")
        .agg(
          firstAs("affected_status"),
          firstAs("affected_status_code")
        )

      df
        .withColumn("clinical_impression_id", explode($"clinical_impressions"))
        .join(affectedStatusByClinicalImpression, "clinical_impression_id")
        .withColumn("is_proband", affectedStatusByClinicalImpression("patient_id") === df("patient_id"))
        .groupBy(affectedStatusByClinicalImpression("patient_id"), $"analysis_service_request_id")
        .agg(
          firstAs("affected_status"),
          firstAs("affected_status_code"),
          firstAs("is_proband")
        )
    }

    def filterUrlColumns: DataFrame = {
      val withUrlColumns: DataFrame = GenomicFiles.foldLeft(df) { case (currDf, file) =>
        currDf.withColumn(file.urlColumn, when($"type" === file.dataType and $"format" === file.format, $"url"))
      }

      val filterCondition: Column = GenomicFiles
        .map(file => !col(file.urlColumn).isNull)
        .reduce((a, b) => a or b)

      val columnsToAgg: List[Column] = GenomicFiles
        .map(file => (file.urlColumn, collect_set(file.urlColumn) as file.urlColumn))
        .map { case (urlColumn, c) => when(size(c) > 0, c).otherwise(null) as urlColumn } // Replace empty lists by null

      withUrlColumns
        .filter(filterCondition)
        .groupBy("patient_id", "fhir_specimen_id")
        .agg(
          columnsToAgg.head,
          columnsToAgg.tail: _*
        )
    }

    def withDocuments(documents: DataFrame): DataFrame = {
      val documentsWithUrls = documents
        .select(
          $"patient_id",
          $"specimen_id" as "fhir_specimen_id",
          $"type",
          explode($"contents") as "content",
        )
        .withColumn("url", $"content.s3_url")
        .withColumn("format", $"content.format")
        .filterUrlColumns

      val columnsToAgg: List[Column] = GenomicFiles.map(file => firstAs(file.urlColumn, ignoreNulls = true))

      df
        .join(documentsWithUrls, df("patient_id") === documentsWithUrls("patient_id") and array_contains($"fhir_specimen_ids", $"fhir_specimen_id"), "left")
        .groupBy(df("patient_id"), $"service_request_id")
        .agg(
          firstAs("specimen_id"),
          firstAs("sample_id") +: columnsToAgg: _*
        )
    }

    def withParentAliquotIds: DataFrame = {
      val motherIds = df.select(
        $"analysis_service_request_id",
        $"patient_id" as "mother_id",
        $"aliquot_id" as "mother_aliquot_id"
      )

      val fatherIds = df.select(
        $"analysis_service_request_id",
        $"patient_id" as "father_id",
        $"aliquot_id" as "father_aliquot_id"
      )

      df
        .join(motherIds, Seq("analysis_service_request_id", "mother_id"), "left")
        .join(fatherIds, Seq("analysis_service_request_id", "father_id"), "left")
    }
  }

  @main
  def run(rc: RuntimeETLContext): Unit = {
    EnrichedClinical(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
