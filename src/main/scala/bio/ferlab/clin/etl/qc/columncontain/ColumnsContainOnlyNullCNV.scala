package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullCNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainOnlyNull(
        cnv_centric,
        cnv_centric.columns.filterNot(List("old_multi_allelic", "practitioner_role_id").contains(_)): _*
      ),
      shouldNotContainOnlyNull(
        cnv_centric.select(explode($"genes")).select("col.*")
      ),
    )
  }
}
