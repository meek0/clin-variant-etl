package bio.ferlab.clin.etl.qc.varsome

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object VariantsShouldBeAnnotated extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_Vari = variants
      .filter(""" size(panels) != 0 """)
      .filter(length($"reference") <= 200 and length($"alternate") <= 200)
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_Vars = varsome.join(variants, Seq("chromosome", "start", "reference", "alternate"), "inner")
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    handleErrors(
      shouldBeEmpty(
        df_Vars.join(df_Vari, Seq("chromosome", "start", "reference", "alternate"), "left_anti")
      )
    )
  }
}
