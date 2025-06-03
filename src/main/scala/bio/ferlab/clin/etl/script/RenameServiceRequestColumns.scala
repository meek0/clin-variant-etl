package bio.ferlab.clin.etl.script

import bio.ferlab.clin.etl.utils.transformation.DatasetTransformationMapping
import bio.ferlab.clin.etl.script.schema.SchemaUtils.{runUpdateSchemaFor, runVacuumFor}
import  bio.ferlab.clin.etl.utils.transformation.RenameFieldsInArrayStruct
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.transformation.{Rename, Transformation}
import bio.ferlab.datalake.spark3.utils.DeltaUtils.vacuum
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, transform => column_transform} // to avoid conflict with another transform function



case class RenameServiceRequestColumns(rc: RuntimeETLContext) {

  private def needsMigration(datasetId: String): Boolean = {
    val source: DatasetConf = rc.config.getDataset(datasetId)
    val sourceLocation = source.location(rc.config)
    val sourceDf: DataFrame = LoadResolver.read(rc.spark, rc.config)(source.format).apply(source)

    /**
    * Returns true if the string representation of the dataset schema (catalogString)
    * contains the substring "service_request_id", which matches both "service_request_id"
    * and "analysis_service_request_id" columns. Note that we use schema.catalogString
    * to ensure the check includes nested fields as well.
    */
    sourceDf.schema.catalogString.contains("service_request_id")
  }

  def run(vacuum: Boolean = false): Unit = {
    runUpdateSchemaFor(
      rc,
      filter = needsMigration,
      RenameServiceRequestColumnsMapping
    )
    if (vacuum) {
      runVacuumFor(rc, RenameServiceRequestColumnsMapping.mapping.keys.toSeq, 1)
    }
  }
}


object RenameServiceRequestColumnsMapping extends DatasetTransformationMapping {
  val rename_service_request_id: List[Transformation] = List(
    Rename(Map("service_request_id" -> "sequencing_id"))
  )

  val rename_analysis_service_request_id: List[Transformation] = List(
    Rename(Map("analysis_service_request_id" -> "analysis_id"))
  )

  val rename_all_service_request_columns: List[Transformation] = 
    rename_service_request_id ++ rename_analysis_service_request_id


  override val mapping: Map[String, List[Transformation]] = Map(
    // fhir
    "enriched_clinical" ->  rename_all_service_request_columns,

    // nextflow
    "nextflow_svclustering_parental_origin" -> rename_all_service_request_columns,

    // normalized
    "normalized_snv" -> rename_all_service_request_columns,
    "normalized_snv_somatic" -> rename_all_service_request_columns,
    "normalized_cnv" -> rename_all_service_request_columns,
    "normalized_cnv_somatic_tumor_only" -> rename_all_service_request_columns,
    "normalized_coverage_by_gene" -> rename_service_request_id,

    // enriched
    "enriched_snv" -> rename_all_service_request_columns,
    "enriched_snv_somatic" -> rename_all_service_request_columns,
    "enriched_cnv" -> rename_all_service_request_columns,
    "enriched_coverage_by_gene" -> rename_service_request_id,
    "enriched_variants" -> List(
      RenameFieldsInArrayStruct("donors", Map("service_request_id" -> "sequencing_id", "analysis_service_request_id" -> "analysis_id"))
    )
  )
}
