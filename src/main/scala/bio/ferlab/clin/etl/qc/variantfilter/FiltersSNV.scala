package bio.ferlab.clin.etl.qc.variantfilter

import bio.ferlab.clin.etl.qc.TestingApp

object FiltersSNV extends TestingApp {
  run { spark =>
    import spark.implicits._
    
    val df = normalized_snv
      .filter($"alternate" === "*")

    shouldBeEmpty(df, "La table devrait etre vide")
  }

}
