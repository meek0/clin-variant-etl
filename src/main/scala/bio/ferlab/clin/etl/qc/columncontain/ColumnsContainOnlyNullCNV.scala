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
        cnv_centric.columns.filterNot(List("old_multi_allelic", "practitioner_role_id", "ciend"/*CLIN-1192*/, "cipos"/*CLIN-1192*/, "analysis_display_name"/*CLIN-1356*/).contains(_)): _*
      ),
      shouldNotContainOnlyNull(
        cnv_centric.select(explode($"genes")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        cnv_centric.select("cluster.*")
      ),
      shouldNotContainOnlyNull(
        cnv_centric.select("cluster.external_frequencies.*")
      ),
      shouldNotContainOnlyNull(
        cnv_centric.select("cluster.frequency_RQDM.germ.*")
      ),
      shouldNotContainOnlyNull(
        cnv_centric.select("cluster.frequency_RQDM.som.*")
      ),
    )
  }
}
