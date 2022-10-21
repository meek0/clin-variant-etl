package bio.ferlab.clin.etl.qc.variantfilter

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object FiltersSNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        normalized_snv.filter($"alternate" === "*")
      )
    )
  }
}
