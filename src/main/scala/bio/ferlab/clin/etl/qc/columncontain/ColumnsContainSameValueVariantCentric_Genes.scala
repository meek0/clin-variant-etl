package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_Genes extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select(explode($"col.orphanet")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select(explode($"col.hpo")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select(explode($"col.omim")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select($"col.gnomad.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"genes")).select($"col.spliceai.*")
      ),
    )
  }
}
