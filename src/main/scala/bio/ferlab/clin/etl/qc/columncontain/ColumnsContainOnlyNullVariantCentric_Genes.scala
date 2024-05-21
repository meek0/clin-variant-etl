package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullVariantCentric_Genes extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select(explode($"col.orphanet")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select(explode($"col.hpo")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select(explode($"col.omim")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select($"col.gnomad.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"genes")).select($"col.spliceai.*")
      ),
    )
  }
}
