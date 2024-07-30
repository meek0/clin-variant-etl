package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locusColumnNames
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_Consequences extends TestingApp {
  run { spark =>
    import spark.implicits._

    val consequencesCols = variants_consequences.select("col.*").drop(locusColumnNames: _*)

    handleErrors(
      shouldNotContainSameValue(
        consequencesCols,
        consequencesCols.columns.filterNot(List("feature_type", "picked").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variants_consequences.select("col.predictions.*")
      ),
    )
  }
}
