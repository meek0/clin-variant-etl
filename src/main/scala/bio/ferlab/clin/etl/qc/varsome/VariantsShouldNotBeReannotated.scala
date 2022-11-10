package bio.ferlab.clin.etl.qc.varsome

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object VariantsShouldNotBeReannotated extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_Vari = variants
      .filter(to_date(col("created_on"),"yyyy-MM-dd") >= date_add(current_date(), -7))
      .select($"chromosome", $"start", $"reference", $"alternate", $"created_on")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_Vars = varsome
      .select($"chromosome", $"start", $"reference", $"alternate", $"updated_on")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    handleErrors(
      shouldBeEmpty(
        df_Vari.join(df_Vars, Seq("chromosome", "start", "reference", "alternate"), "inner")
          .filter(to_date(col("created_on"),"yyyy-MM-dd") =!= to_date(col("updated_on"),"yyyy-MM-dd"))
      )
    )
  }
}
