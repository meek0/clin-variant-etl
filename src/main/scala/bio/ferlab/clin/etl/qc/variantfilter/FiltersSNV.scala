package bio.ferlab.clin.etl.qc.variantfilter

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp.shouldBeEmpty
object FiltersSNV extends TestingApp {
  run { spark =>
    import spark.implicits._
    
    val df = normalized_snv
      .filter($"alternate" === "*")

    shouldBeEmpty(df, "La table devrait etre vide")
  }

}
