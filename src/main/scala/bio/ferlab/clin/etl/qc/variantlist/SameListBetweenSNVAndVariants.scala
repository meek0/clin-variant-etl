package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp

object SameListBetweenSNVAndVariants extends TestingApp {

  import spark.implicits._

  val df_NorSNV = normalized_snv
    .select($"chromosome", $"start", $"reference", $"alternate")
    .dropDuplicates("chromosome", "start", "reference", "alternate")

  val df_NorVar = variants
    .select($"chromosome", $"start", $"reference", $"alternate")
    .dropDuplicates("chromosome", "start", "reference", "alternate")

  val df_Diff = df_NorSNV.join(df_NorVar, Seq("chromosome", "start", "reference", "alternate"), "left_anti")

  shouldBeEmpty(df_Diff, "La table devrait etre vide")

}
