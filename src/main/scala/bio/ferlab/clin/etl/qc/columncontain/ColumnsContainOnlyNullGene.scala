package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullGene extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainOnlyNull(
        gene_centric
      ),
      shouldNotContainOnlyNull(
        gene_centric.select(explode($"orphanet")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        gene_centric.select(explode($"hpo")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        gene_centric.select(explode($"omim")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        gene_centric.select(explode($"ddd")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        gene_centric.select(explode($"cosmic")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        gene_centric.select(explode($"number_of_snvs_per_patient")).select("col.*")
      ),
    )
  }
}
