package bio.ferlab.clin.etl.script

import bio.ferlab.clin.etl.utils.transformation.{DatasetTransformationMapping, EnrichWithClinicalInfo, Limit}
import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.transformation.{Lit, Transformation}

case class RepartitionByAnalysisId(rc: RuntimeETLContext) extends UpdatePartitioning(rc) {

  override val mapping = new DatasetTransformationMapping() {

    // The new partitioning will take effect when the dataset is overwritten
    // with the updated configuration.
    // NOTE: For the normalized_variants table, you must also run the etl_migrate DAG to re-ingest and re-aggregate the data
    override val mapping: Map[String, List[Transformation]] = Map(
      "normalized_snv" -> List(),
      "normalized_exomiser" -> List(
        EnrichWithClinicalInfo(
          clinicalDf = rc.config.getDataset("enriched_clinical").read(rc.config, rc.spark),
          joinCols = Seq("aliquot_id"),
          clinicalCols = Seq("analysis_id")
        )
      ),
      "normalized_coverage_by_gene" -> List(
        EnrichWithClinicalInfo(
          clinicalDf = rc.config.getDataset("enriched_clinical").read(rc.config, rc.spark),
          joinCols = Seq("batch_id", "sequencing_id"),
          clinicalCols = Seq("analysis_id", "bioinfo_analysis_code")
        )
      ),
      "normalized_variants" -> List(
        Limit(0), // Will create an empty table with the new partitioning
        Lit("", "analysis_id", "bioinfo_analysis_code")
      )
    )
  }
}