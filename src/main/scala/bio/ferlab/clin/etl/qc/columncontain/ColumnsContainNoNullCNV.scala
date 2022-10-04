package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
//import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ColumnsContainNoNullCNV extends TestingApp {

  run { spark =>
    import spark.implicits._

    var df = cnv_centric
    List("aliquot_id", "chromosome", "start", "reference", "alternate").foreach{
      c => assert(df.select(col(c)).filter(col(c).isNull).count() != 0, "La colonne ne devrait pas contenir de Null")
    }

    df = cnv_centric.select(explode($"genes")).select("col.*")
    List("overlap_exons").foreach{
      c => assert(df.select(col(c)).filter(col(c).isNull).count() != 0, "La colonne ne devrait pas contenir de Null")
    }
  }
}
