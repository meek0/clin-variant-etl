package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp

object NonDuplicationVariantCentric extends TestingApp {

  import spark.implicits._

  val df = variant_centric.groupBy($"chromosome", $"start", $"reference", $"alternate")
    .count
    .filter($"count" > 1)

  shouldBeEmpty(df, "La table devrait etre vide")

}
