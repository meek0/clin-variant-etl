package bio.ferlab.clin.etl.script

import bio.ferlab.clin.etl.utils.transformation.DatasetTransformationMapping
import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.transformation.{Drop, Rename, Transformation}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

case class RepartitionByAnalysisId(rc: RuntimeETLContext) extends UpdatePartitioning(rc) {

  override val mapping = new DatasetTransformationMapping() {

    // The new partitioning will take effect when the dataset is overwritten
    // with the updated configuration.
    override val mapping: Map[String, List[Transformation]] = Map(

      "normalized_franklin" -> List(
        Rename(Map("analysis_id" -> "franklin_analysis_id")),
        EnrichFranklinWithClinicalInfo(
          clinicalDf = rc.config.getDataset("enriched_clinical").read(rc.config, rc.spark),
        ),
        Drop("batch_id", "family_id")
      )
    )
  }
}

case class EnrichFranklinWithClinicalInfo(clinicalDf: DataFrame) extends Transformation {

  override def transform: DataFrame => DataFrame = { df =>
    // If the aliquot id is null, we assume it is a family franklin analysis and we rely on the 
    // family_id and batch_id to retrieve the analysis_id
    val familyDf = clinicalDf.select("analysis_id", "batch_id", "family_id").filter(col("family_id").isNotNull).distinct()
    val withoutAliquotIdDf = df.filter(col("aliquot_id").isNull)
    val outputWithoutAliquotIdDf = withoutAliquotIdDf
      .join(
        familyDf,
        withoutAliquotIdDf("batch_id") === familyDf("batch_id") && withoutAliquotIdDf("family_id") === familyDf("family_id"),
        "left"
      )
      .select(withoutAliquotIdDf("*"), familyDf("analysis_id"))

    // Otherwise, we use the batch_id and the aliquot_id
    val aliquotIdDf = clinicalDf.select("analysis_id", "batch_id", "aliquot_id").distinct()
    val withAliquotIdDf = df.filter(col("aliquot_id").isNotNull)
    val outputWithAliquotIdDf = withAliquotIdDf
      .join(
        aliquotIdDf,
        withAliquotIdDf("batch_id") === aliquotIdDf("batch_id") && withAliquotIdDf("aliquot_id") === aliquotIdDf("aliquot_id"),
        "left"
      )
      .select(withAliquotIdDf("*"), aliquotIdDf("analysis_id"))

    outputWithAliquotIdDf.unionAll(outputWithoutAliquotIdDf)
  }
}

