package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainNoNullCoverageByGene extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainNull(
        coverage_by_gene,
        "gene", "aliquot_id", "batch_id"
      ),
    )
  }
}
