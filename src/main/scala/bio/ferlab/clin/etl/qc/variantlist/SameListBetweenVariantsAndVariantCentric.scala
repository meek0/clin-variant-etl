package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV.run
import bio.ferlab.clin.etl.qc.TestingApp.shouldBeEmpty
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
