package bio.ferlab.clin.etl.qc.varsome

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object VariantsShouldNotBeReannotated extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_Vari_ET = variants
    .withColumn("elapsed_time", datediff(current_date(), (to_date(col("created_on"),"yyyy-MM-dd"))))

    val min_elapsed_time = df_Vari_ET
    .agg(min("elapsed_time")).head()(0)

    val df_Vari = df_Vari_ET
    .filter(""" size(panels) != 0 """)
    .filter($"elapsed_time" === min_elapsed_time)
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
