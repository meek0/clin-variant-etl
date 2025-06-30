package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locusColumnNames
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullVariantCentric_Consequences extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainOnlyNull(
        variants_consequences.select("col.*").drop(locusColumnNames: _*)
      ),
      shouldNotContainOnlyNull(
        variants_consequences.select("col.predictions.*")
      ),
    )
  }
}
