package bio.ferlab.clin.etl.qc.varsome

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object VariantsShouldBeReannotated extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_Vars = varsome
      .filter(to_date(col("updated_on"),"yyyy-MM-dd") < date_add(current_date(), -7))
      .select($"chromosome", $"start", $"reference", $"alternate", $"updated_on")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    handleErrors(
      shouldBeEmpty(
        df_Vars
      )
    )
  }
}
