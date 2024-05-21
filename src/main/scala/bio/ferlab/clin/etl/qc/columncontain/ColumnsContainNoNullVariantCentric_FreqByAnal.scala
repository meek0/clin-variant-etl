package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainNoNullVariantCentric_FreqByAnal extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainNull(
        variant_centric.select(explode($"frequencies_by_analysis") as "frequencies_by_analysis").select($"frequencies_by_analysis.analysis_code" as "analysis_code")
      ),
    )
  }
}
