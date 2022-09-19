package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV.run

// Test:               Validation de la liste des variants entre la table variants et la table variant_centric
// Objectifs du test:  Vérifier que la liste des variants dans la table variants est la même que dans la table variant_centric
// Critère de succès:  La table doit être vide

object SameListBetweenVariantsAndVariantCentric extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_Var = variants
    .select($"chromosome", $"start", $"reference", $"alternate")
    .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_VarCen = variant_centric
    .select($"chromosome", $"start", $"reference", $"alternate")
    .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_Diff = df_Var.unionAll(df_VarCen).except(df_Var.intersect(df_VarCen))

    shouldBeEmpty(df_Diff, "La table devrait etre vide")
  }

}
