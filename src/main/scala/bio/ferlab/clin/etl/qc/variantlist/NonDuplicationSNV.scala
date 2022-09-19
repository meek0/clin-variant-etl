package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp

// Test:               Les variants doivent être unique par patient_id
// Objectifs du test:  Vérifier que les variants ne sont pas dupliqués dans la table normalized_snv
// Critère de succès:  La table doit être vide

object NonDuplicationSNV extends TestingApp {
  run { spark =>
    import spark.implicits._
    val df = normalized_snv.groupBy($"chromosome", $"start", $"reference", $"alternate", $"patient_id").count
      .filter($"count" > 1)

    shouldBeEmpty(df, "La table devrait etre vide")
  }

}
