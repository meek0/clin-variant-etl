package bio.ferlab.clin.etl.qc.varsome

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object VariantsShouldNotBeAnnotated extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_VarNoPanel = variants
      .filter(""" size(panels) = 0 """)
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_VarRefAltLength = variants
      .filter(""" size(panels) != 0 """)
      .filter(length($"reference") > 200 or length($"alternate") > 200)
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_Vari = df_VarNoPanel
      .join(df_VarRefAltLength, Seq("chromosome", "start", "reference", "alternate"), "full")
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_Vars = varsome
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    handleErrors(
      shouldBeEmpty(
        df_Vari.join(df_Vars, Seq("chromosome", "start", "reference", "alternate"), "inner")
      )
    )
  }
}
