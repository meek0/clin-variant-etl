package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueGene extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        gene_centric
      ),
      shouldNotContainSameValue(
        gene_centric.select(explode($"orphanet")).select("col.*")
      ),
      shouldNotContainSameValue(
        gene_centric.select(explode($"hpo")).select("col.*")
      ),
      shouldNotContainSameValue(
        gene_centric.select(explode($"omim")).select("col.*")
      ),
      shouldNotContainSameValue(
        gene_centric.select(explode($"ddd")).select("col.*")
      ),
      shouldNotContainSameValue(
        gene_centric.select(explode($"cosmic")).select("col.*")
      ),
      shouldNotContainSameValue(
        gene_centric.select(explode($"number_of_snvs_per_patient")).select("col.*")
      ),
    )
  }
}
