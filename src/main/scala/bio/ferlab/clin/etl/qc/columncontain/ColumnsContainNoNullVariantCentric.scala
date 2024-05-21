package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainNoNullVariantCentric extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainNull(
        variant_centric,
        "chromosome", "start", "reference", "alternate"
      ),
    )
  }
}
