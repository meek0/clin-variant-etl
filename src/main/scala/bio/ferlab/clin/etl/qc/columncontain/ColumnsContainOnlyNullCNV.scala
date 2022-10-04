package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullCNV.run
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullCNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    var df = cnv_centric
    df.columns.filterNot(List("old_multi_allelic", "practitioner_role_id").contains(_)).foreach{
      c => assert(df.select(col(c)).filter(col(c).isNotNull).count() != 0, "La colonne ne devrait pas contenir que des Nulls")
    }

    df = cnv_centric.select(explode($"genes")).select("col.*")
    df.columns.filterNot(List().contains(_)).foreach{
      c => assert(df.select(col(c)).filter(col(c).isNotNull).count() != 0, "La colonne ne devrait pas contenir que des Nulls")
    }
  }
}
