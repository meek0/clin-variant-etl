package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object SameListBetweenSNVSomaticAndVariants extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_SNVSomatic = snv
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_Var = variants
      .dropDuplicates("chromosome", "start", "reference", "alternate")
      .select($"chromosome", $"start", $"reference", $"alternate")

    handleErrors(
      shouldBeEmpty(
        df_SNVSomatic.join(df_Var, Seq("chromosome", "start", "reference", "alternate"), "left_anti")
      )
    )
  }
}
